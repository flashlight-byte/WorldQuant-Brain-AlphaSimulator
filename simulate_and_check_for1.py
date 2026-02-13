# Alpha仿真和回测检查脚本
# 功能：对一个alpha表达式进行仿真，然后进行回测检查，通过指标后打上SUCCESS标签以便后续提交
import requests
from requests.auth import HTTPBasicAuth
import json
import time
import pandas as pd
from datetime import datetime
from os.path import expanduser


def sign_in():
    """登录WorldQuant Brain API"""
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
    print(f"登录状态: {response.status_code}")
    if response.status_code == 200:
        print(f"用户ID: {response.json().get('user', {}).get('id', 'N/A')}")
    return sess


def requests_wq(s, type='get', url='', json_data=None, t=15):
    """封装请求函数，处理重试和错误"""
    session = s
    while True:
        try:
            if type == 'get':
                ret = session.get(url)
            elif type == 'post':
                if json_data is None:
                    ret = session.post(url)
                else:
                    ret = session.post(url, json=json_data)
            elif type == 'patch':
                ret = session.patch(url, json=json_data)
            
            if ret.status_code == 429:
                print(f"状态={ret.status_code}, 延时{t}秒")
                time.sleep(t)
                continue
            if ret.status_code in (200, 201):
                return ret, session
            if ret.status_code == 401:
                print("认证失败，重新登录...")
                session = sign_in()
                continue
            else:
                print(f"\033[31m状态={ret.status_code}，continue\033[0m")
                time.sleep(5)
                continue
        except requests.RequestException as e:
            print(f"请求错误: {e}. 重试中...")
            time.sleep(10)
            session = sign_in()
    return None, None


def simulate_alpha(sess, expression, settings=None):
    """
    仿真一个alpha表达式
    
    Args:
        sess: 会话对象
        expression: alpha表达式字符串
        settings: 仿真设置字典，如果为None则使用默认设置
    
    Returns:
        alpha_id: 仿真完成后的alpha ID
    """
    if settings is None:
        settings = {
            'instrumentType': 'EQUITY',
            'region': 'USA',
            'universe': 'TOP3000',
            'delay': 1,
            'decay': 5,
            'neutralization': 'SUBINDUSTRY',
            'truncation': 0.08,
            'pasteurization': 'ON',
            'unitHandling': 'VERIFY',
            'nanHandling': 'ON',
            'language': 'FASTEXPR',
            'visualization': False,
        }
    
    simulation_data = {
        'type': 'REGULAR',
        'settings': settings,
        'regular': expression
    }
    
    print(f"开始仿真Alpha表达式: {expression}")
    print(f"仿真设置: {settings}")
    
    sim_resp, sess = requests_wq(
        sess,
        'post',
        'https://api.worldquantbrain.com/simulations',
        json_data=simulation_data
    )
    
    # 这里一般是表达式/参数级别的错误（语法、不可用运算符等），状态码多为 4xx
    if sim_resp.status_code not in (200, 201):
        print(f"仿真请求失败: HTTP {sim_resp.status_code}")
        try:
            err_body = sim_resp.json()
            print("仿真错误明细(JSON):")
            print(json.dumps(err_body, indent=2, ensure_ascii=False))
            # 常见字段尝试单独打印，方便快速查看
            for key in ["error", "message", "detail"]:
                if key in err_body:
                    print(f"{key}: {err_body[key]}")
        except Exception:
            # 如果不是 JSON，就直接打印文本
            print("仿真错误响应内容:")
            print(sim_resp.text)
        return None, sess
    
    sim_progress_url = sim_resp.headers.get('Location')
    if not sim_progress_url:
        print("无法获取仿真进度URL")
        return None, sess
    
    print("等待仿真完成...")
    # 等待仿真完成
    while True:
        sim_progress_resp, sess = requests_wq(sess, 'get', sim_progress_url)
        if sim_progress_resp.status_code != 200:
            print(f"获取仿真进度失败: HTTP {sim_progress_resp.status_code}")
            try:
                print("进度接口错误响应(JSON):")
                print(json.dumps(sim_progress_resp.json(), indent=2, ensure_ascii=False))
            except Exception:
                print("进度接口错误响应内容:")
                print(sim_progress_resp.text)
            return None, sess
        
        retry_after_sec = float(sim_progress_resp.headers.get("Retry-After", 0))
        if retry_after_sec == 0:  # simulation done!模拟完成!
            break
        print(f"仿真进行中，等待 {retry_after_sec} 秒...")
        time.sleep(retry_after_sec)
    
    # 仿真完成后检查返回体，既尝试拿 alpha，也把状态和错误打印出来
    try:
        body = sim_progress_resp.json()
    except Exception:
        print("仿真完成但进度接口返回不是合法 JSON，原始内容为：")
        print(sim_progress_resp.text)
        return None, sess

    # 一般会有 status / state / errors / message 等字段
    status = body.get("status") or body.get("state")
    if status and status not in ("COMPLETE", "SUCCESS"):
        print(f"仿真未成功结束，状态: {status}")
        # 常见错误字段
        for key in ["error", "message", "detail"]:
            if key in body:
                print(f"{key}: {body[key]}")
        # 有些接口会把多条错误放在 errors 数组里
        if "errors" in body:
            print("errors:")
            print(json.dumps(body["errors"], indent=2, ensure_ascii=False))
        # 直接打印整个 body 方便你对照网页上的提示
        print("仿真进度响应(JSON):")
        print(json.dumps(body, indent=2, ensure_ascii=False))
        return None, sess

    alpha_id = body.get("alpha")
    if not alpha_id:
        print("仿真完成，但响应中未找到 alpha ID，完整响应为：")
        print(json.dumps(body, indent=2, ensure_ascii=False))
        return None, sess

    print(f"仿真完成！Alpha ID: {alpha_id}")
    return alpha_id, sess


def get_check_submission(s, alpha_id):
    """
    检查Alpha提交状态
    
    Args:
        s: 会话对象
        alpha_id: Alpha ID
    
    Returns:
        check_result: 检查结果 ("SUCCESS", "ERROR", "FAIL", "nan", "sleep")
        sess: 会话对象
    """
    sess = s
    while True:
        result, sess = requests_wq(sess, 'get', 
                                    f"https://api.worldquantbrain.com/alphas/{alpha_id}/check")
        if "retry-after" in result.headers:
            retry_after = float(result.headers["Retry-After"])
            print(f"等待检查结果，延时 {retry_after} 秒...")
            time.sleep(retry_after)
        else:
            break
    
    if result.json().get("is", 0) == 0:
        print(f"Alpha {alpha_id}: 未登录或数据未准备好，返回 'sleep'")
        return "sleep", sess
    
    checks_df = pd.DataFrame(result.json()["is"]["checks"])
    
    # 检查 SELF_CORRELATION 是否为 "nan"
    self_correlation_value = checks_df[checks_df["name"] == "SELF_CORRELATION"]["value"].values[0]
    
    if any(checks_df["result"] == "ERROR"):
        print(f"Alpha {alpha_id}: \033[31m ERROR \033[0m，检查失败")
        return "ERROR", sess
    
    if any(checks_df["result"] == "FAIL"):
        print(f"Alpha {alpha_id}: \033[31m FAIL \033[0m，检查失败")
        return "FAIL", sess
    
    if pd.isna(self_correlation_value) or str(self_correlation_value).lower() == "nan":
        print(f"Alpha {alpha_id}: SELF_CORRELATION 为 \033[31m nan \033[0m，检查失败")
        return "nan", sess
    
    # 所有检查都通过
    print(f"Alpha {alpha_id}: \033[32m 所有检查通过 \033[0m")
    return "SUCCESS", sess


def set_alpha_properties(s, alpha_id, name=None, color=None, 
                        selection_desc="None", combo_desc="None",
                        tags="SUCCESS", regular_desc="None"):
    """
    设置Alpha属性，包括标签
    
    Args:
        s: 会话对象
        alpha_id: Alpha ID
        name: Alpha名称
        color: 颜色
        selection_desc: 选择描述
        combo_desc: 组合描述
        tags: 标签（默认"SUCCESS"）
        regular_desc: 常规描述
    
    Returns:
        response: API响应
        sess: 会话对象
    """
    sess = s
    params = {
        "color": color,
        "name": name,
        "tags": [tags],
        "category": None,
        "regular": {"description": regular_desc},
        "combo": {"description": combo_desc},
        "selection": {"description": selection_desc},
    }
    response, sess = requests_wq(sess, 'patch', 
                                  f"https://api.worldquantbrain.com/alphas/{alpha_id}",
                                  json_data=params)
    return response, sess


def get_alpha_info(s, alpha_id):
    """获取Alpha的详细信息"""
    sess = s
    response, sess = requests_wq(sess, 'get', 
                                  f"https://api.worldquantbrain.com/alphas/{alpha_id}")
    if response.status_code == 200:
        return response.json(), sess
    return None, sess


def main():
    """主函数：仿真Alpha并进行回测检查"""
    print("=" * 50)
    print("Alpha仿真和回测检查脚本")
    print(f"开始时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 50)
    
    # 登录
    sess = sign_in()
    if not sess:
        print("登录失败，程序退出")
        return
    
    # 配置Alpha表达式（可以修改这里来测试不同的表达式）
    # 写法要求：① 用三引号 """ """ 包裹多行；② 语句以分号分隔；③ 最后一句为 alpha 输出
    alpha_expression = """
iv_diff = ts_delta(implied_volatility_call_120, 1);
iv_signal = ts_decay_linear(iv_diff, 5);
iv_z = zscore(winsorize(iv_signal, std = 3));
cap_bucket = bucket(rank(cap), range = "0.2, 1, 0.3");
iv_rank = group_rank(iv_z, cap_bucket);
alpha = group_neutralize(iv_rank, subindustry);
alpha
""".strip()
    
    # 可选：自定义仿真设置
    custom_settings = None  # 使用默认设置，如需自定义可取消注释下面的代码
    custom_settings = {
        'instrumentType': 'EQUITY',
        'region': 'USA',
        'universe': 'TOP3000',
        'delay': 1,
        'decay': 4,
        'neutralization': 'SECTOR',
        'truncation': 0.08,
        'pasteurization': 'ON',
        'unitHandling': 'VERIFY',
        'nanHandling': 'ON',
        'language': 'FASTEXPR',
        'visualization': False,
    }
    
    # 步骤1: 仿真Alpha
    print("\n" + "=" * 50)
    print("步骤1: 开始仿真Alpha")
    print("=" * 50)
    alpha_id, sess = simulate_alpha(sess, alpha_expression, custom_settings)
    
    if not alpha_id:
        print("仿真失败，程序退出")
        return
    
    # 等待一段时间，确保Alpha数据已准备好
    print("\n等待Alpha数据准备...")
    time.sleep(10)
    
    # 步骤2: 获取Alpha信息并显示指标
    print("\n" + "=" * 50)
    print("步骤2: 获取Alpha信息")
    print("=" * 50)
    alpha_info, sess = get_alpha_info(sess, alpha_id)
    if alpha_info:
        is_data = alpha_info.get("is", {})
        print(f"Sharpe: {is_data.get('sharpe', 'N/A')}")
        print(f"Fitness: {is_data.get('fitness', 'N/A')}")
        print(f"Turnover: {is_data.get('turnover', 'N/A')}")
        print(f"Margin: {is_data.get('margin', 'N/A')}")
        print(f"Long Count: {is_data.get('longCount', 'N/A')}")
        print(f"Short Count: {is_data.get('shortCount', 'N/A')}")
    
    # 步骤3: 进行回测检查
    print("\n" + "=" * 50)
    print("步骤3: 进行回测检查")
    print("=" * 50)
    
    # 重试机制：最多尝试3次
    max_retries = 3
    check_result = None
    for attempt in range(max_retries):
        check_result, sess = get_check_submission(sess, alpha_id)
        if check_result != "sleep":
            break
        if attempt < max_retries - 1:
            print(f"Alpha数据未准备好，等待40秒后重试 ({attempt + 1}/{max_retries})...")
            time.sleep(40)
    
    # 步骤4: 根据检查结果处理
    print("\n" + "=" * 50)
    print("步骤4: 处理检查结果")
    print("=" * 50)
    
    if check_result == "SUCCESS":
        print(f"\n\033[32m✓ Alpha {alpha_id} 检查通过！\033[0m")
        print("正在打上SUCCESS标签...")
        response, sess = set_alpha_properties(
            sess, 
            alpha_id, 
            name=datetime.now().strftime("%Y.%m.%d"),
            tags="SUCCESS",
            regular_desc=f"Simulated and checked on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        )
        if response and response.status_code in (200, 201):
            print(f"\033[32m✓ 成功为Alpha {alpha_id} 打上SUCCESS标签\033[0m")
            print("可以在平台上查看Tag-SUCCESS，并手动提交")
        else:
            print(f"\033[33m警告: 打标签可能失败，状态码: {response.status_code if response else 'None'}\033[0m")
    else:
        print(f"\n\033[31m✗ Alpha {alpha_id} 检查未通过\033[0m")
        print(f"检查结果: {check_result}")
        if check_result == "sleep":
            print("Alpha数据可能还未准备好，请稍后手动检查")
        elif check_result in ("ERROR", "FAIL", "nan"):
            print("Alpha未通过回测检查，不符合提交条件")
    
    print("\n" + "=" * 50)
    print(f"完成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 50)


if __name__ == "__main__":
    main()
