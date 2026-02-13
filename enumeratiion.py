# 同 world3.py，多了横截面运算符 rank
import requests
import json
from time import sleep
from os.path import expanduser
from requests.auth import HTTPBasicAuth

def sign_in():
    # Load credentials # 加载凭证
    with open(expanduser('brain_credentials.txt')) as f:
        credentials = json.load(f)

    # Extract username and password from the list # 从列表中提取用户名和密码
    username, password = credentials

    # Create a session object # 创建会话对象，用于在多次请求之间保持状态和连接
    sess = requests.Session()

    # Set up basic authentication # 设置基本身份验证
    sess.auth = HTTPBasicAuth(username, password)

    # Send a POST request to the API for authentication # 向API发送POST请求进行身份验证
    response = sess.post('https://api.worldquantbrain.com/authentication')

    # Print response status and content for debugging # 打印响应状态和内容以调试
    print(response.status_code)
    print(response.json())
    return sess

sess = sign_in()

# 获取数据集ID为fundamental6（Company Fundamental Data for Equity）下的所有数据字段
### Get Data_fields like Data Explorer 获取所有满足条件的数据字段及其ID
def get_datafields(
        s,
        searchScope,
        dataset_id: str = '',
        search: str = ''
):
    import pandas as pd
    instrument_type = searchScope['instrumentType']
    region = searchScope['region']
    delay = searchScope['delay']
    universe = searchScope['universe']
    # 以下 URL 根据官网 API 文档生成
    if len(search) == 0:
        url_template = "https://api.worldquantbrain.com/data-fields?" + \
                       f"instrumentType={instrument_type}" + \
                       f"&region={region}&delay={str(delay)}&universe={universe}&dataset.id={dataset_id}&limit=50" + \
                       "&offset={x}"
        max_retries = 5
        retry_count = 0
        while retry_count < max_retries:
            response = s.get(url_template.format(x=0))
            if response.status_code == 429:  # API 限流
                retry_after = int(response.headers.get('Retry-After', 60))
                print(f"API 限流，等待 {retry_after} 秒后重试...")
                sleep(retry_after)
                retry_count += 1
                continue
            elif response.status_code != 200:
                print(f"错误: 获取数据字段总数失败，状态码: {response.status_code}")
                print(f"响应内容: {response.text}")
                retry_count += 1
                if retry_count < max_retries:
                    sleep(10)
                    continue
                else:
                    return pd.DataFrame()  # 返回空 DataFrame
            try:
                response_json = response.json()
                if 'count' not in response_json:
                    print(f"错误: 响应中没有 'count' 键")
                    print(f"响应内容: {response_json}")
                    return pd.DataFrame()  # 返回空 DataFrame
                count = response_json['count']
                break  # 成功获取 count，退出重试循环
            except json.JSONDecodeError:
                print(f"警告: JSON 解析失败")
                retry_count += 1
                if retry_count < max_retries:
                    sleep(5)
                    continue
                else:
                    return pd.DataFrame()  # 返回空 DataFrame
    else:
        url_template = "https://api.worldquantbrain.com/data-fields?" + \
                       f"instrumentType={instrument_type}" + \
                       f"&region={region}&delay={str(delay)}&universe={universe}&limit=50" + \
                       f"&search={search}" + \
                       "&offset={x}"
        count = 100

    datafields_list = []
    for x in range(0, count, 50):
        max_retries = 5
        retry_count = 0
        while retry_count < max_retries:
            datafields = s.get(url_template.format(x=x))
            # 检查响应状态
            if datafields.status_code == 429:  # API 限流
                retry_after = int(datafields.headers.get('Retry-After', 60))
                print(f"API 限流，等待 {retry_after} 秒后重试... (offset={x})")
                sleep(retry_after)
                retry_count += 1
                continue
            elif datafields.status_code != 200:
                print(f"警告: 请求失败，状态码: {datafields.status_code}")
                print(f"响应内容: {datafields.text}")
                retry_count += 1
                if retry_count < max_retries:
                    sleep(10)
                    continue
                else:
                    break
            # 检查响应内容
            try:
                response_json = datafields.json()
                if 'results' not in response_json:
                    print(f"警告: 响应中没有 'results' 键")
                    print(f"响应内容: {response_json}")
                    break
                # `results` 是 API 响应中的一个关键字段，包含数据字段列表
                datafields_list.append(response_json['results'])
                break  # 成功获取数据，退出重试循环
            except json.JSONDecodeError:
                print(f"警告: JSON 解析失败")
                retry_count += 1
                if retry_count < max_retries:
                    sleep(5)
                    continue
                else:
                    break

    # 将嵌套列表展平
    datafields_list_flat = [item for sublist in datafields_list for item in sublist]
    
    if len(datafields_list_flat) == 0:
        print("警告: 没有获取到任何数据字段")
        return pd.DataFrame()  # 返回空 DataFrame

    # 转化为二维数据表，便于筛选、查询
    datafields_df = pd.DataFrame(datafields_list_flat)
    return datafields_df


# 定义搜索范围
searchScope = {'region': 'USA', 'delay': '1', 'universe': 'TOP3000', 'instrumentType': 'EQUITY'}
# 从数据集中获取数据字段
# fnd6 的列（columns）可能包括：
# - id: 数据字段ID（如 "ebitda", "revenue"）
# - type: 数据类型（如 "MATRIX", "SCALAR"）
# - name: 字段名称
# - dataset: 所属数据集
# - description: 描述
# - ... 其他元数据字段

# 示例数据：
#        id          type    name              dataset
# 0      ebitda     MATRIX  EBITDA            fundamental6
# 1      revenue    MATRIX  Revenue           fundamental6
# 2      assets     MATRIX  Total Assets      fundamental6
# ...
fnd6 = get_datafields(s=sess, searchScope=searchScope, dataset_id='fundamental6')
# 过滤类型为 "MATRIX" 的数据字段
fnd6 = fnd6[fnd6['type'] == "MATRIX"]
# 提取 ID 列，转为 numpy 数组
datafields_list_fnd6 = fnd6['id'].values
# 输出数据字段的ID列表
# print(datafields_list_fnd6)
print(len(datafields_list_fnd6))

# group_neutralize(ts_rank(rank(fnd6_acdo)/rank(enterprise_value), 5), industry)
# 模板
# group_neutralize(<ts_compare_op>(rank(<company_fundamentals>)/rank(enterprise_value),<days>),<group>)

# 定义时间序列比较操作符
ts_compare_op = ['ts_rank']  # 时间序列比较操作符列表
# 定义时间周期列表
days = [5, 65, 252]
# 定义分组依据列表
group = ['subindustry']
# 定义公司基本面数据的字段列表
company_fundamentals = datafields_list_fnd6
# 初始化alpha表达式列表
alpha_expressions = []
# 遍历时间序列比较操作符
for tco in ts_compare_op:
    # 遍历公司基本面数据的字段
    for cf in company_fundamentals:
        # 遍历时间周期
        for d in days:
            # 遍历分组依据
            for grp in group:
                # 生成alpha表达式并添加到列表中，同时保留分组信息
                expr = f"group_neutralize({tco}(rank({cf}) / rank(enterprise_value), {d}), {grp})"
                alpha_expressions.append((expr, grp))

# 输出生成的alpha表达式总数 # 打印或返回结果字符串列表
print(f"there are total {len(alpha_expressions)} alpha expressions")

# 打印结果（仅打印表达式部分的前5个）
print([e for e, _ in alpha_expressions][:5])
print(len(alpha_expressions))

# 将datafield替换到Alpha模板(框架)中group_rank({fundamental model data}/cap,subindustry)批量生成Alpha
alpha_list = []

print("将alpha表达式与setting封装")
for index, alpha_tuple in enumerate(alpha_expressions, start=1):
    expr, grp = alpha_tuple
    print(f"正在循环第 {index} 个元素,组装alpha表达式: {expr}")
    # 将分组转换为大写以匹配设置中的预期值
    neutral = grp.upper()
    simulation_data = {
        "type": "REGULAR",
        "settings": {
            "instrumentType": "EQUITY",
            "region": "USA",
            "universe": "TOP3000",
            "delay": 1,
            "decay": 5,
            "neutralization": neutral,
            "truncation": 0.05,
            "pasteurization": "ON",
            "unitHandling": "VERIFY",
            "nanHandling": "ON",
            "language": "FASTEXPR",
            "visualization": False,
        },
        "regular": expr
    }
    alpha_list.append(simulation_data)
print(f"there are {len(alpha_list)} Alphas to simulate")

# 输出
print(alpha_list[0])

# 在使用该代码前，需将Course3的Alpha列表里的所有alpha存入csv文件。headers of the csv：type,settings,regular
import csv
import os

# Check if the file exists
alpha_list_file_path = './MyQuantCode/alpha_list_pending_simulated.csv'  # replace with your actual file path
file_exists = os.path.isfile(alpha_list_file_path)

# Write the list of dictionaries to a CSV file, when append keep the original header
# 注意：settings 是嵌套字典，需要转换为 JSON 字符串才能写入 CSV
with open(alpha_list_file_path, 'a', newline='', encoding='utf-8') as output_file:
    dict_writer = csv.DictWriter(output_file, fieldnames=['type', 'settings', 'regular'])
    # If the file does not exist, write the header
    if not file_exists:
        dict_writer.writeheader()

    # 将 settings 字典转换为 JSON 字符串
    for alpha in alpha_list:
        alpha_for_csv = {
            'type': alpha['type'],
            'settings': json.dumps(alpha['settings']),  # 将嵌套字典转为 JSON 字符串
            'regular': alpha['regular']
        }
        dict_writer.writerow(alpha_for_csv)

print("Alpha list has been saved to alpha_list_pending_simulated.csv")

# 将Alpha一个一个发送至服务器进行回测,并检查是否断线，如断线则重连
##设置log
import logging
# Configure the logging setting
logging.basicConfig(filename='./MyQuantCode/simulation.log', level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

alpha_fail_attempt_tolerance = 15 # 每个alpha允许的最大失败尝试次数
is_submit = False  # 标志变量，用于控制是否提交alpha
if is_submit:
    # 从第0个元素开始迭代回测alpha_list
    for index in range(0, len(alpha_list)):
        alpha = alpha_list[index]
        print(f"{index}: {alpha['regular']}")
        logging.info(f"{index}: {alpha['regular']}")
        keep_trying = True  # 控制while循环继续的标志
        failure_count = 0  # 记录失败尝试次数的计数器

        while keep_trying:
            try:
                # 尝试发送POST请求
                sim_resp = sess.post(
                    'https://api.worldquantbrain.com/simulations',
                    json=alpha  # 将当前alpha（一个JSON）发送到服务器
                )

                # 从响应头中获取位置
                sim_progress_url = sim_resp.headers['Location']
                logging.info(f'Alpha location is: {sim_progress_url}')  # 记录位置
                print(f'Alpha location is: {sim_progress_url}')  # 打印位置
                keep_trying = False  # 成功获取位置，退出while循环

            except Exception as e:
                # 处理异常：记录错误，让程序休眠15秒后重试
                logging.error(f"No Location, sleep 15 and retry, error message: {str(e)}")
                print("No Location, sleep 15 and retry")
                sleep(15)  # 休眠15秒后重试
                failure_count += 1  # 增加失败尝试次数

                # 检查失败尝试次数是否达到容忍上限
                if failure_count >= alpha_fail_attempt_tolerance:
                    sess = sign_in()  # 重新登录会话
                    failure_count = 0  # 重置失败尝试次数
                    logging.error(f"No location for too many times, move to next alpha {alpha['regular']}")  # 记录错误
                    print(f"No location for too many times, move to next alpha {alpha['regular']}")  # 打印信息
                    break  # 退出while循环，移动到for循环中的下一个alpha
