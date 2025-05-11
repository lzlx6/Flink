import pymysql


def insert_data():
    # 连接数据库
    connection = pymysql.connect(
        host='cdh03',
        port=3306,
        user='root',
        password='root',
        database='realtime_v2',
        charset='utf8mb4'
    )
    cursor = connection.cursor()

    try:
        with open('national-code.txt', 'r', encoding='utf-8') as file:
            for line in file:
                data = line.strip().split(',')
                code = data[0]
                province = data[1] if len(data) > 1 else ''
                city = data[2] if len(data) > 2 else ''
                area = data[3] if len(data) > 3 else ''

                sql = "INSERT INTO spider_national_code_compare_dic (code, province, city, area) VALUES (%s, %s, %s, %s)"
                cursor.execute(sql, (code, province, city, area))

        connection.commit()
        print("数据插入成功！")
    except FileNotFoundError:
        print("文件未找到！")
    except Exception as e:
        print(f"发生错误: {e}")
        connection.rollback()
    finally:
        cursor.close()
        connection.close()


if __name__ == "__main__":
    insert_data()