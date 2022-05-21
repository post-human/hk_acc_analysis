import datetime
import logging
import os
import re
import sqlite3

import xlrd as xlrd
from kivy.app import App
from kivy.lang import Builder
from kivy.uix.boxlayout import BoxLayout
from kivy.uix.button import Button
from kivy.uix.filechooser import FileChooserListView
from kivy.uix.screenmanager import Screen, ScreenManager
from xlrd import xldate_as_datetime

SUBJECT_CODE_PATTERN = "^\\d{7}$"


def init_log(log_file):
    logging.basicConfig(  # 针对 basicConfig 进行配置(basicConfig 其实就是对 logging 模块进行动态的调整，之后可以直接使用)
        level=logging.DEBUG,  # INFO 等级以下的日志不会被记录
        format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',  # 日志输出格式
        filename=log_file,  # 日志存放路径(存放在当前相对路径)
        # filemode=mode,  # 输入模式；如果当前我们文件已经存在，可以使用 'a' 模式替代 'w' 模式
        # 与文件写入的模式相似，'w' 模式为没有文件时创建文件；'a' 模式为追加内容写入日志文件
    )


init_log('acc.log')


class MainWindow(Screen):
    pass


class AnalysisWindow(Screen):
    pass


def typeof(variate):
    var_type = None
    if isinstance(variate, int):
        var_type = "int"
    elif isinstance(variate, str):
        var_type = "str"
    elif isinstance(variate, float):
        var_type = "float"
    elif isinstance(variate, list):
        var_type = "list"
    elif isinstance(variate, tuple):
        var_type = "tuple"
    elif isinstance(variate, dict):
        var_type = "dict"
    elif isinstance(variate, set):
        var_type = "set"
    return var_type


def save_data(path, filename):
    data = xlrd.open_workbook(os.path.join(path, filename[0]))
    table = data.sheets()[0]
    logging.info(table.name)
    acc_id = None
    data_dict = {}
    subject_dict = {}
    curr_subject = None

    for row_num in range(table.nrows):
        row_cell_0 = table.cell_value(row_num, 0)
        if 'str' == typeof(row_cell_0):
            if len(row_cell_0) == 0:
                continue
            if row_num == 0:
                acc_id = int(table.cell_value(row_num, 1))
                continue
            if re.match(SUBJECT_CODE_PATTERN, row_cell_0):
                curr_subject = row_cell_0
                subject_dict[curr_subject] = table.cell_value(row_num, 1)
                data_dict[curr_subject] = {}  # key:month value:[debit,credit]
        else:
            period = xldate_as_datetime(row_cell_0, 0).strftime('%Y%m')
            period_val = data_dict[curr_subject].setdefault(period, [0, 0])
            # get data
            debit = table.cell_value(row_num, 7)
            is_credit = typeof(debit) != 'float'
            if is_credit:
                credit = table.cell_value(row_num, 8)
                period_val[1] = period_val[1] + credit
            else:
                period_val[0] = period_val[0] + debit

    subject_codes = list(subject_dict.keys())
    conn = sqlite3.connect('hk.db')
    conn.set_trace_callback(print)
    # * check subject_code, insert if not exist, get sid
    with conn:
        cursor = conn.execute(
            "SELECT hs.sid,hs.code FROM hk_subject hs WHERE hs.code in ({0}) and hs.acc_id={1} and hs.is_del = 0".format(
                ', '.join('?' for _ in subject_codes), acc_id), subject_codes)
        results = cursor.fetchall()
        subject_id_dict = {}
        for result in results:
            subject_id_dict[result[1]] = result[0]
        for subject_code in subject_codes:
            if subject_code not in subject_id_dict:
                # insert if not exist
                cursor.execute("insert into hk_subject(acc_id,code,name,create_time) values (?,?,?,?)",
                               [acc_id, subject_code, subject_dict[subject_code], datetime.datetime.now()])
                subject_id = cursor.lastrowid
                subject_id_dict[subject_code] = subject_id
        insert_list = []
        for code, v in data_dict.items():
            data_insert_sql = "insert into hk_subject_actual_mount(sid, period, debit, credit, create_time) " \
                              "values (?,?,?,?,?)"
            sid = subject_id_dict[code]
            for period, amounts in v.items():
                data_check_sql = "select hsam.sid,hsam.debit,hsam.credit from hk_subject_actual_mount hsam " \
                                 "where hsam.sid=? and hsam.period=?"
                cursor = conn.execute(data_check_sql, [sid, period])
                results = cursor.fetchall()
                if len(results) == 0:
                    insert_list.append([sid, period, amounts[0], amounts[1], datetime.datetime.now()])
                else:
                    # todo
                    pass
            if len(insert_list) > 0:
                conn.executemany(data_insert_sql, insert_list)
        print(results)
        print(subject_id_dict)


class AccManager(ScreenManager):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.container = None

    def open_file(self):
        self.container = BoxLayout(orientation='vertical')
        filechooser = FileChooserListView()
        filechooser.bind(on_selection=lambda x: self.selected(filechooser.selection))

        open_btn = Button(text='open', size_hint=(1, .2))
        open_btn.bind(on_release=lambda x: self.open(filechooser.path, filechooser.selection))

        self.container.add_widget(filechooser)
        self.container.add_widget(open_btn)
        self.get_root_window().add_widget(self.container)

    def open(self, path, filename):
        if len(filename) > 0 and filename[0].endswith('.xls'):
            save_data(path, filename)
        self.get_root_window().remove_widget(self.container)

    @staticmethod
    def selected(filename):
        logging.info("selected: %s" % filename[0])

    def show_bar(self):
        pass


kv = Builder.load_file('acc.kv')


class AccApp(App):
    def build(self):
        return kv


if __name__ == '__main__':
    AccApp().run()
