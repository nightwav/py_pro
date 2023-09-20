import platform
import pymysql
import queue
import threading
from decimal import Decimal
import time
import datetime
import logging
from pymodbus.client.sync import ModbusSerialClient
from logging.handlers import RotatingFileHandler

# 创建主控编号对象
room_num: int = 22  # 机房
master_num: int = 1  # 主控编号

# pdu读取参数
pdu_channels = 16  # pdu通道数

# 本地数据库
server_db_conn = pymysql.connect(host='192.168.0.2',
                                 user='root',
                                 password='yantuo57831066',
                                 database='dynamic',
                                 port=3306)
# 远端数据库
local_db_conn = pymysql.connect(host='127.0.0.1',
                                user='root',
                                password='xilu57831066',
                                database='dynamic_xl',
                                port=3306,
                                charset='utf8')

# 一个 Queue 对象数组用于存储写线圈请求
write_queue_list: list = [
    queue.Queue(),
    queue.Queue(),
    queue.Queue(),
    queue.Queue(),
    queue.Queue(),
    queue.Queue(),
    queue.Queue()
]

# 创建主 logger 对象
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)  # 设置主 logger 的日志级别为最低级别 DEBUG

# 创建 INFO 级别的 RotatingFileHandler 并设置相关配置
info_file_handler = RotatingFileHandler('info.log',
                                        mode='a',
                                        maxBytes=1024 * 1024,
                                        backupCount=1,
                                        encoding='utf-8')
info_file_handler.setLevel(logging.INFO)  # 设置 INFO 级别输出到该处理器上
info_file_formatter = logging.Formatter(
    '%(asctime)s - %(levelname)s - %(message)s')
info_file_handler.setFormatter(info_file_formatter)
info_file_handler.addFilter(
    lambda record: record.levelno <= logging.INFO)  # 添加过滤器，只处理 INFO 及以下级别的日志

# 创建 ERROR 级别的 RotatingFileHandler 并设置相关配置
error_file_handler = RotatingFileHandler('error.log',
                                         mode='a',
                                         maxBytes=1024 * 1024,
                                         backupCount=1,
                                         encoding='utf-8')
error_file_handler.setLevel(logging.ERROR)  # 设置 ERROR 级别输出到该处理器上
error_file_formatter = logging.Formatter(
    '%(asctime)s - %(levelname)s - %(message)s')
error_file_handler.setFormatter(error_file_formatter)
error_file_handler.addFilter(
    lambda record: record.levelno >= logging.ERROR)  # 添加过滤器，只处理 ERROR 及以上级别的日志

# 将处理器添加到 logger 对象
logger.addHandler(info_file_handler)
logger.addHandler(error_file_handler)


def get_serial_port(serial_num):
    """
    在不同的操作系统上选择串口设备路径
    """
    if platform.system() == 'Windows':
        # Windows 系统
        return f'COM{serial_num}'
    elif platform.system() == 'Linux':
        # Linux 系统
        return f'/dev/ttymxc{serial_num}'
    else:
        # 其他操作系统
        return None


def ChangeNum(num_arry):
    """
    将两个十进制整数 转换位 4位16进制 后再转为一位10进制
    :param num_arry: 两个整数数组
    :return: 运算结果
    """
    hex_num1 = hex(num_arry[0])[2:].zfill(2)
    hex_num2 = hex(num_arry[1])[2:].zfill(2)
    hex_str = hex_num1 + hex_num2
    hex_num = int(hex_str, base=16)
    return hex_num


def generate_id(port, address):
    """
    生成pdu id
    :param port: pdu连接主控端口
    :param address: pdu地址
    :return: PDUID
    """
    id = (
        f"R{room_num}Z{master_num}S{port.replace('/dev/ttymxc','').replace('COM','')}PDU{str(address).zfill(5)}"
    )
    return id


def get_current_time():
    """
    :return: 当前的日期和时间yyyy-MM-dd HH:ss:mm
    """
    # 获取当前的日期和时间
    now = datetime.datetime.now()

    # 将日期和时间按照 yyyy-MM-dd HH:ss:mm 的格式进行组合并返回
    return now.strftime("%Y-%m-%d %H:%M:%S")


def run_sql_data(db_connect, sql_str):
    """
    运行添加、删除、修改语句
    :param db_connect: 数据库服务
    :param sql_str: sql语句
    :return: 
    """
    cursor = db_connect.cursor()
    try:
        cursor.execute(sql_str)
        db_connect.commit()
    except Exception as e:
        logger.error(f"insert_data Error: {e}")
    finally:
        # 关闭数据库连接
        cursor.close()
        # db_connect.close()


def select_data(db_connect, sql_query):
    """
    运行查询语句
    :param db_connect: 服务器配置
    :param sql_query: sql语句
    :return: 
    """
    cur = db_connect.cursor()
    res = ()
    try:
        cur.execute(sql_query)
        res = cur.fetchall()
    except Exception as e:
        logger.error(f"select_data Error: {e}")
    finally:
        # 关闭数据库连接
        cur.close()
        return res


def insert_qurey_str(address, com, data_arr):
    """
    生成插入语句
    :param address: 服务器配置
    :param com: 串口
    :param data_arr: 要插入的数据数组
    :return: 
    """
    sql_query = ''
    values_str = ','.join(str(x) for x in data_arr)

    # 拼接 SQL 查询语句并执行
    pdu_cols = "`dDateTime`, `iControllerNumber`,`cSerialName`, `cType`, `cCode`, `iAddress`, `iChannelsAmount`, `bLocked`, `bCommError`"
    pdu_cols += ", `dCountVoltage`, `dCountCurrent`, `dCountActivePower`, `dCountApparentPower`, `dCountReactivePower`, `dCountPowerFactor`, `dCountActiveEnergy`"
    pdu_cols += ", `dVoltage1`, `dCurrent1`, `dActivePower1`, `dApparentPower1`, `dReactivePower1`, `dPowerFactor1`, `dActiveEnergy1`, `iVoltageLoadStatus1`, `iCurrentLoadStatus1`, `bSwitchStatus1`"
    pdu_cols += ", `dVoltage2`, `dCurrent2`, `dActivePower2`, `dApparentPower2`, `dReactivePower2`, `dPowerFactor2`, `dActiveEnergy2`, `iVoltageLoadStatus2`, `iCurrentLoadStatus2`, `bSwitchStatus2`"
    pdu_cols += ", `dVoltage3`, `dCurrent3`, `dActivePower3`, `dApparentPower3`, `dReactivePower3`, `dPowerFactor3`, `dActiveEnergy3`, `iVoltageLoadStatus3`, `iCurrentLoadStatus3`, `bSwitchStatus3`"
    pdu_cols += ", `dVoltage4`, `dCurrent4`, `dActivePower4`, `dApparentPower4`, `dReactivePower4`, `dPowerFactor4`, `dActiveEnergy4`, `iVoltageLoadStatus4`, `iCurrentLoadStatus4`, `bSwitchStatus4`"
    pdu_cols += ", `dVoltage5`, `dCurrent5`, `dActivePower5`, `dApparentPower5`, `dReactivePower5`, `dPowerFactor5`, `dActiveEnergy5`, `iVoltageLoadStatus5`, `iCurrentLoadStatus5`, `bSwitchStatus5`"
    pdu_cols += ", `dVoltage6`, `dCurrent6`, `dActivePower6`, `dApparentPower6`, `dReactivePower6`, `dPowerFactor6`, `dActiveEnergy6`, `iVoltageLoadStatus6`, `iCurrentLoadStatus6`, `bSwitchStatus6`"
    pdu_cols += ", `dVoltage7`, `dCurrent7`, `dActivePower7`, `dApparentPower7`, `dReactivePower7`, `dPowerFactor7`, `dActiveEnergy7`, `iVoltageLoadStatus7`, `iCurrentLoadStatus7`, `bSwitchStatus7`"
    pdu_cols += ", `dVoltage8`, `dCurrent8`, `dActivePower8`, `dApparentPower8`, `dReactivePower8`, `dPowerFactor8`, `dActiveEnergy8`, `iVoltageLoadStatus8`, `iCurrentLoadStatus8`, `bSwitchStatus8`"
    pdu_cols += ", `dVoltage9`, `dCurrent9`, `dActivePower9`, `dApparentPower9`, `dReactivePower9`, `dPowerFactor9`, `dActiveEnergy9`, `iVoltageLoadStatus9`, `iCurrentLoadStatus9`, `bSwitchStatus9`"
    pdu_cols += ", `dVoltage10`, `dCurrent10`, `dActivePower10`, `dApparentPower10`, `dReactivePower10`, `dPowerFactor10`, `dActiveEnergy10`, `iVoltageLoadStatus10`, `iCurrentLoadStatus10`, `bSwitchStatus10`"
    pdu_cols += ", `dVoltage11`, `dCurrent11`, `dActivePower11`, `dApparentPower11`, `dReactivePower11`, `dPowerFactor11`, `dActiveEnergy11`, `iVoltageLoadStatus11`, `iCurrentLoadStatus11`, `bSwitchStatus11`"
    pdu_cols += ", `dVoltage12`, `dCurrent12`, `dActivePower12`, `dApparentPower12`, `dReactivePower12`, `dPowerFactor12`, `dActiveEnergy12`, `iVoltageLoadStatus12`, `iCurrentLoadStatus12`, `bSwitchStatus12`"
    pdu_cols += ", `dVoltage13`, `dCurrent13`, `dActivePower13`, `dApparentPower13`, `dReactivePower13`, `dPowerFactor13`, `dActiveEnergy13`, `iVoltageLoadStatus13`, `iCurrentLoadStatus13`, `bSwitchStatus13`"
    pdu_cols += ", `dVoltage14`, `dCurrent14`, `dActivePower14`, `dApparentPower14`, `dReactivePower14`, `dPowerFactor14`, `dActiveEnergy14`, `iVoltageLoadStatus14`, `iCurrentLoadStatus14`, `bSwitchStatus14`"
    pdu_cols += ", `dVoltage15`, `dCurrent15`, `dActivePower15`, `dApparentPower15`, `dReactivePower15`, `dPowerFactor15`, `dActiveEnergy15`, `iVoltageLoadStatus15`, `iCurrentLoadStatus15`, `bSwitchStatus15`"
    pdu_cols += ", `dVoltage16`, `dCurrent16`, `dActivePower16`, `dApparentPower16`, `dReactivePower16`, `dPowerFactor16`, `dActiveEnergy16`, `iVoltageLoadStatus16`, `iCurrentLoadStatus16`, `bSwitchStatus16`"
    pdu_cols += ", `dVoltage17`, `dCurrent17`, `dActivePower17`, `dApparentPower17`, `dReactivePower17`, `dPowerFactor17`, `dActiveEnergy17`, `iVoltageLoadStatus17`, `iCurrentLoadStatus17`, `bSwitchStatus17`"
    pdu_cols += ", `dVoltage18`, `dCurrent18`, `dActivePower18`, `dApparentPower18`, `dReactivePower18`, `dPowerFactor18`, `dActiveEnergy18`, `iVoltageLoadStatus18`, `iCurrentLoadStatus18`, `bSwitchStatus18`"
    pdu_cols += ", `dVoltage19`, `dCurrent19`, `dActivePower19`, `dApparentPower19`, `dReactivePower19`, `dPowerFactor19`, `dActiveEnergy19`, `iVoltageLoadStatus19`, `iCurrentLoadStatus19`, `bSwitchStatus19`"
    pdu_cols += ", `dVoltage20`, `dCurrent20`, `dActivePower20`, `dApparentPower20`, `dReactivePower20`, `dPowerFactor20`, `dActiveEnergy20`, `iVoltageLoadStatus20`, `iCurrentLoadStatus20`, `bSwitchStatus20`"
    pdu_cols += ", `dVoltage21`, `dCurrent21`, `dActivePower21`, `dApparentPower21`, `dReactivePower21`, `dPowerFactor21`, `dActiveEnergy21`, `iVoltageLoadStatus21`, `iCurrentLoadStatus21`, `bSwitchStatus21`"
    pdu_cols += ", `dVoltage22`, `dCurrent22`, `dActivePower22`, `dApparentPower22`, `dReactivePower22`, `dPowerFactor22`, `dActiveEnergy22`, `iVoltageLoadStatus22`, `iCurrentLoadStatus22`, `bSwitchStatus22`"
    pdu_cols += ", `dVoltage23`, `dCurrent23`, `dActivePower23`, `dApparentPower23`, `dReactivePower23`, `dPowerFactor23`, `dActiveEnergy23`, `iVoltageLoadStatus23`, `iCurrentLoadStatus23`, `bSwitchStatus23`"
    pdu_cols += ", `dVoltage24`, `dCurrent24`, `dActivePower24`, `dApparentPower24`, `dReactivePower24`, `dPowerFactor24`, `dActiveEnergy24`, `iVoltageLoadStatus24`, `iCurrentLoadStatus24`, `bSwitchStatus24`"
    pdu_cols += ", `iDefine1`, `iDefine2`, `iDefine3`, `iDefine4`, `iDefine5`, `cDefine6`, `cDefine7`, `cDefine8`, `cDefine9`, `cDefine10`"

    string_to_copy = "0.0, 0.000, 0.0, 0.0, 0.0, 0.000, 0.0, 1, 1, 0"
    pdu17_24 = ", ".join([string_to_copy] * 8)

    sql_query = f"INSERT INTO collect_pdu ({pdu_cols}) VALUES ('{get_current_time()}',{master_num},'{com.replace('/dev/ttymxc','BUS_').replace('COM','BUS_')}','PDC-003214VRB','{generate_id(port=com, address=address)}',{address},16, 1, 0,{values_str},{pdu17_24}, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)"

    return sql_query


def read_pdu(client, slave_address):  # slave_address 从机地址 每个pdu的地址
    """
    生成插入语句
    :param client: 串口客户端
    :param slave_address: 从机地址 pdu的地址
    :return: NULL
    """
    try:
        data = []
        # 读取表头14位
        result = client.read_input_registers(
            address=0,
            count=14,  # 表头
            unit=slave_address)
        if result.isError():
            logger.error(
                f"read_pdu port: {client.port} address {slave_address} error: {result}"
            )
            return

        # register_list = response.registers[0:2]  # 电压 电流
        data.append(Decimal(result.registers[0]) / Decimal(10))  # 电压
        data.append(Decimal(result.registers[1]) / Decimal(1000))  # 电流
        data.append(Decimal(ChangeNum(result.registers[2:4])) /
                    Decimal(10))  # 有功功率
        data.append(Decimal(ChangeNum(result.registers[4:6])) /
                    Decimal(10))  # 视在功率
        data.append(Decimal(ChangeNum(result.registers[6:8])) /
                    Decimal(10))  # 无功功率
        data.append(Decimal(result.registers[8]) / Decimal(1000))  # 功率因数
        data.append(Decimal(ChangeNum(result.registers[9:11])) /
                    Decimal(10))  # 总电能

        # print("Data:", data)
        # 读取各通道开关状态 pdu_channels 通道数量
        coils_status = client.read_coils(address=0,
                                         count=pdu_channels,
                                         unit=slave_address).bits
        # 读取除表头外所有通道 pdu_channels 通道数量
        total_registers = 13 * pdu_channels
        registers_per_read = 120
        start_address = 14
        registers = []
        for i in range(start_address, start_address + total_registers,
                       registers_per_read):

            count = min(registers_per_read,
                        start_address + total_registers - i)

            result = client.read_input_registers(address=i,
                                                 count=count,
                                                 unit=slave_address)
            if result.isError():
                logger.error("Failed to read input registers:", result)
                break
            else:
                registers += result.registers

        # 组合表头、通道数据、通道开关数据
        for n in range(pdu_channels):
            register = registers[n * 13:13 + n * 13]  # 单通道数据13位

            data.append(Decimal(register[0]) / Decimal(10))  # 电压
            data.append(Decimal(register[1]) / Decimal(1000))  # 电流
            data.append(Decimal(ChangeNum(register[2:4])) /
                        Decimal(10))  # 有功功率
            data.append(Decimal(ChangeNum(register[4:6])) /
                        Decimal(10))  # 视在功率
            data.append(Decimal(ChangeNum(register[6:8])) /
                        Decimal(10))  # 无功功率
            data.append(Decimal(register[8]) / Decimal(1000))  # 功率因数
            data.append(Decimal(ChangeNum(register[9:11])) /
                        Decimal(10))  # 总电能
            # 0.正常 1.负载非法脱离 2.负载非法接入 4.硬件故障 8.硬件离线
            # 0.正常 1.过压 2.欠压 4.过流
            data += register[-2:]

            # 开关状态
            data.append(coils_status[n])

        # print("Data:", data)
        insert_qurey = insert_qurey_str(address=slave_address,
                                        com=client.port,
                                        data_arr=data)

        local_insert_qurey = insert_qurey.replace('collect_pdu',
                                                  'collect_pdu_history')
        # print(insert_qurey)
        # print(local_insert_qurey)
        run_sql_data(db_connect=server_db_conn, sql_str=insert_qurey)
        run_sql_data(db_connect=local_db_conn, sql_str=local_insert_qurey)
    except Exception as e:
        logger.error(f"{client.port} pdu address {slave_address} error: {e}")


def read_registers(port_number, begin, end):
    """
    生成插入语句
    :param port_number: 串口号 1-5
    :param begin: 起始从机地址（包含本身）
    :param end: 结束从机地址（包含本身）
    :return: 
    """
    # 连接 modbus 设备并创建一个客户端对象
    client = ModbusSerialClient(method='rtu',
                                port=get_serial_port(port_number),
                                baudrate=9600,
                                timeout=1)
    is_serial_exception = False  # 初始化标志位，默认值为 False
    while True:
        if client.connect():
            is_serial_exception = False

            # 写线圈队列
            _queue = write_queue_list[port_number]
            try:
                # 读取 modbus 寄存器值...
                for i in range(begin, end + 1):
                    read_pdu(client=client, slave_address=i)
                    time.sleep(0.2)
                    # 判断写线圈队列是否为空
                    if not _queue.empty():
                        request = _queue.get(block=False)
                        if int(str(request[1])) > 0:
                            client.write_coil(
                                address=request[1],  # channel number
                                value=request[2],  # value
                                unit=request[0])  # pdu address
                        else:  # 地址送0 所有通道全关全开
                            values = []
                            for i in range(0, 16):
                                values.append(request[2])

                            client.write_coils(address=0,
                                               values=values,
                                               unit=request[0])
                        # 将执行过的开关数据更新为完成
                        run_sql_data(
                            server_db_conn,
                            f"UPDATE collect_pdu_set SET bReceived=1,bFinished = 1 WHERE iAutoId = {request[3]}"
                        )
                        time.sleep(0.1)
            except queue.Empty:
                time.sleep(1)
            # finally:
            #     server_db_conn.close()
        else:
            if not is_serial_exception:  # 只有第一次发现异常时才进行输出
                logger.error(f' {get_serial_port(port_number)} 不可用')
                is_serial_exception = True
            time.sleep(60)


def write_coils_queue():
    """
    写线圈操作（通道开关控制）
    :return: 
    """
    while True:

        try:
            # 执行 SQL 查询语句
            sql = f"SELECT iAutoId,cSerialName,iAddress,iChannelsNumber,bOpen FROM collect_pdu_set where iControllerNumber={master_num} AND bFinished=0"
            # 获取查询结果并打印出来
            results = select_data(db_connect=server_db_conn, sql_query=sql)
            # print("results", len(results))
            # delete_ids = []
            for row in results:
                # delete_ids.append(str(row[0]))  # 第一列为 ID 字段
                # 第二列为 串口名称 字段  不同串口存在不同的串口队列中
                _queue = write_queue_list[int(row[1])]
                # 将新的写入请求添加到队列中以供处理线程使用
                slave_address = row[2]  # 第三列为 PDU（从机）地址 字段
                channel_num = row[3] - 1  # 第四列为 开关编号 字段 地址为编号减一
                value = int(str(row[4])[-2])  # 第五列 开关量 字段
                # 加入队列
                _queue.put((slave_address, channel_num, value, row[0]))

            # 从数据库删除已插入队列的数据
            # if len(delete_ids) > 0:
            #     delete_sql = f"DELETE FROM collect_pdu_set WHERE iAutoId IN ({','.join(delete_ids)})"
            #     run_sql_data(db_connect=server_db, sql_str=delete_sql)

        except Exception as e:
            logging.error(f"write_coils_queue Error: {e}")
        finally:
            time.sleep(3)
            # server_db_conn.close()


# 启动两个子进程分别执行不同的任务
if __name__ == "__main__":
    logger.info(f'程序启动时间{get_current_time()}.')
    # 读pdu线程 每个串口一个单独线程 0号串口1保留
    t1 = threading.Thread(target=read_registers, args=(1, 1, 32))
    t1.start()

    t2 = threading.Thread(target=read_registers, args=(2, 1, 32))
    t2.start()

    t3 = threading.Thread(target=read_registers, args=(3, 1, 32))
    t3.start()

    t4 = threading.Thread(target=read_registers, args=(4, 1, 32))
    t4.start()

    t5 = threading.Thread(target=read_registers, args=(5, 1, 32))
    t5.start()

    # 写线圈队列线程
    t_write_coils_queue = threading.Thread(target=write_coils_queue)
    t_write_coils_queue.start()
