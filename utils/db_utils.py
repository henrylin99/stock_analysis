import pymysql

class DatabaseUtils:
    # 数据库连接信息 Database connection information
    _host = 'localhost'       # 替换为你的MySQL主机 Replace with your MySQL host
    _user = 'root'   # 替换为你的MySQL用户名 Replace with your MySQL username
    _password = 'test' # 替换为你的MySQL密码 Replace with your MySQL password
    _database = 'stock_analysis'  # 替换为你的MySQL数据库名 Replace with your MySQL database name
    _charset = 'utf8mb4'




    @classmethod
    def connect_to_mysql(cls):
        """
        连接到MySQL数据库 Connecting to MySQL Database
        :return: MySQL连接对象和游标 MySQL Connection Objects and Cursors
        """
        conn = pymysql.connect(
            host=cls._host,
            user=cls._user,
            password=cls._password,
            database=cls._database,
            charset=cls._charset
        )
        cursor = conn.cursor()
        return conn, cursor 