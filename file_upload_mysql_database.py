import os

import pymysql

PATH = '/var/lib/mysql-files/'


class File:
    def __init__(self, host, port, user, passwd, db_name):
        self.db = pymysql.connect(
            host=host,
            port=port,
            user=user,
            password=passwd,
            database=db_name,
            charset='utf8'
        )
        self.cur = self.db.cursor()

    # 创建表
    def create_table(self):
        table_name=input('请输入表名（table_name）：')
        while True:
            yesno=input('是否需要再创建表,若已经创建无需再创建，输入y退出（y/n）:')
            if yesno=='y':
                str01=input('请输入创建表的sql语句（sql）：')
                self.cur.execute(str01)
                try:
                    self.db.commit()
                except Exception as e:
                    print(e)
                    self.db.rollback()
                    continue
            elif yesno=='n':
                break
            else:
                print('输入错误！')
        return  table_name

    # 导入文件
    def leading_in(self,path):
        table_name=self.create_table()
        sql="load data infile '%s'  into table  %s  fields terminated by  ','  lines terminated by '\n'"%(path,table_name)
        print(sql)
        self.cur.execute(sql)
        try:
                    self.db.commit()
        except Exception as e:
                    print(e)
                    self.db.rollback()
                    print("上传失败！")
        else:
            print('上传成功！')



class Leading_in:
    def __init__(self, file_path,host, port, user, passwd, db_name):
        self.file_path = file_path
        self.file_name = self.file_path.split('/')[-1]
        self.db = File(host, port, user, passwd, db_name)

    # 改变文件的权限为666
    def change_file_limit(self):
        os.system('chmod 666 ./%s' % self.file_name)

    # 释放root权限
    def relese_user_limit(self):
        os.system('exit')

    # 移动文件到mysql_files的文件夹中
    def copy_file(self):
        os.system(' sudo cp %s  %s' % (self.file_path, PATH))

    # 文件导入数据库的表中
    def file_leading_in(self):
        new_path = PATH + self.file_name
        self.db.leading_in(new_path)
        print('file_leading_in ok')

    # 删除mysql_files中的文件
    def delete_file(self):
        new_path = PATH + self.file_name
        os.system('sudo rm %s' % new_path)
        print('delete_file ok')

    # 主函数
    def main(self):
        self.copy_file()
        self.relese_user_limit()
        self.file_leading_in()
        self.delete_file()
        self.relese_user_limit()


if __name__ == '__main__':
    file_path = input('请输入文件路径(file_path)：')
    host = input('请输入用户主机名(host)：')
    port = int(input('请输入端口号(port)：'))
    user = input('请输入用户名(user)：')
    passwd = input('请输入密码(password)：')
    db_name = input('请输入调用库名(database_name)：')
    try:
        li = Leading_in(file_path,  host, port, user, passwd, db_name)
        li.main()
    except Exception as e:
        print(e)

