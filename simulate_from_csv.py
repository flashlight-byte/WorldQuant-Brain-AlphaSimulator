# Alpha批量仿真和回测脚本（从CSV读取）- 并发版本
# 功能：从CSV文件读取alpha列表，并发进行仿真和回测，标记已完成的项目，支持断点续跑
# 支持同时运行多个仿真（默认3个并发）
import requests
from requests.auth import HTTPBasicAuth
import json
import time
import pandas as pd
import ast
from datetime import datetime
from os.path import expanduser
import sys
import os
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

# 导入alpha_simulate_and_check.py中的函数
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from simulate_and_check_for1 import (
    sign_in, requests_wq, simulate_alpha, get_check_submission,
    set_alpha_properties, get_alpha_info
)


def load_alpha_list_from_csv(csv_path):
    """
    从CSV文件加载alpha列表
    
    Args:
        csv_path: CSV文件路径
    
    Returns:
        df: DataFrame，包含alpha列表和状态信息
    """
    try:
        df = pd.read_csv(csv_path, encoding='utf-8')
        
        # 如果CSV中没有status列，添加它
        if 'status' not in df.columns:
            df['status'] = 'PENDING'
            df['alpha_id'] = ''
            df['check_result'] = ''
            df['completed_time'] = ''
        
        # 确保所有必要的列都存在
        required_columns = ['type', 'settings', 'regular', 'status']
        for col in required_columns:
            if col not in df.columns:
                raise ValueError(f"CSV文件缺少必要的列: {col}")
        
        return df
    except Exception as e:
        print(f"读取CSV文件失败: {e}")
        raise


# 全局锁，用于保护CSV文件的读写操作
csv_lock = threading.Lock()

def save_alpha_list_to_csv(df, csv_path, use_lock=True):
    """
    保存alpha列表到CSV文件（线程安全）
    
    Args:
        df: DataFrame
        csv_path: CSV文件路径
        use_lock: 是否使用锁（如果调用者已经持有锁，可以设为False）
    """
    def _save():
        try:
            df.to_csv(csv_path, index=False, encoding='utf-8')
        except Exception as e:
            print(f"保存CSV文件失败: {e}")
            raise
    
    if use_lock:
        with csv_lock:
            _save()
    else:
        _save()


def parse_settings(settings_str):
    """
    解析settings字符串（可能是JSON字符串或字典字符串）
    
    Args:
        settings_str: settings字符串
    
    Returns:
        dict: 解析后的settings字典
    """
    try:
        # 尝试作为JSON解析
        return json.loads(settings_str)
    except json.JSONDecodeError:
        try:
            # 尝试作为Python字典字符串解析
            return ast.literal_eval(settings_str)
        except:
            print(f"无法解析settings: {settings_str}")
            return None


def process_single_alpha(alpha_row, row_index, index, total, df, csv_path):
    """
    处理单个alpha：仿真、回测、标记（线程安全版本）
    
    Args:
        alpha_row: DataFrame的一行，包含alpha信息
        row_index: DataFrame中的原始行索引
        index: 当前alpha在处理队列中的索引（从0开始）
        total: 总待处理alpha数量
        df: DataFrame对象（用于更新状态）
        csv_path: CSV文件路径
    
    Returns:
        tuple: (success: bool, alpha_id: str, check_result: str, row_index: int)
    """
    thread_id = threading.current_thread().name
    print("\n" + "=" * 80)
    print(f"[线程 {thread_id}] 处理Alpha [{index + 1}/{total}]")
    print("=" * 80)
    print(f"[线程 {thread_id}] Alpha表达式: {alpha_row['regular']}")
    
    # 每个线程使用独立的session（requests.Session不是线程安全的）
    sess = sign_in()
    if not sess:
        print(f"[线程 {thread_id}] 登录失败")
        return False, None, "LOGIN_FAILED", row_index
    
    # 解析settings
    settings = parse_settings(alpha_row['settings'])
    if settings is None:
        print(f"[线程 {thread_id}] 跳过Alpha [{index + 1}]: settings解析失败")
        return False, None, "SETTINGS_ERROR", row_index
    
    try:
        # 步骤1: 仿真Alpha
        print(f"[线程 {thread_id}] [步骤1] 开始仿真Alpha...")
        alpha_id, sess = simulate_alpha(sess, alpha_row['regular'], settings)
        
        if not alpha_id:
            print(f"[线程 {thread_id}] Alpha [{index + 1}] 仿真失败")
            return False, None, "SIMULATION_FAILED", row_index
        
        # 等待一段时间，确保Alpha数据已准备好
        print(f"[线程 {thread_id}] 等待Alpha数据准备...")
        time.sleep(10)
        
        # 步骤2: 获取Alpha信息并显示指标
        print(f"[线程 {thread_id}] [步骤2] 获取Alpha信息...")
        alpha_info, sess = get_alpha_info(sess, alpha_id)
        if alpha_info:
            is_data = alpha_info.get("is", {})
            print(f"[线程 {thread_id}] Sharpe: {is_data.get('sharpe', 'N/A')}")
            print(f"[线程 {thread_id}] Fitness: {is_data.get('fitness', 'N/A')}")
            print(f"[线程 {thread_id}] Turnover: {is_data.get('turnover', 'N/A')}")
            print(f"[线程 {thread_id}] Margin: {is_data.get('margin', 'N/A')}")
            print(f"[线程 {thread_id}] Long Count: {is_data.get('longCount', 'N/A')}")
            print(f"[线程 {thread_id}] Short Count: {is_data.get('shortCount', 'N/A')}")
        
        # 步骤3: 进行回测检查
        print(f"[线程 {thread_id}] [步骤3] 进行回测检查...")
        
        # 重试机制：最多尝试3次
        max_retries = 3
        check_result = None
        for attempt in range(max_retries):
            check_result, sess = get_check_submission(sess, alpha_id)
            if check_result != "sleep":
                break
            if attempt < max_retries - 1:
                print(f"[线程 {thread_id}] Alpha数据未准备好，等待40秒后重试 ({attempt + 1}/{max_retries})...")
                time.sleep(40)
        
        # 步骤4: 根据检查结果处理
        print(f"[线程 {thread_id}] [步骤4] 处理检查结果...")
        sharpe = is_data.get('sharpe')
        fitness = is_data.get('fitness')
        turnover = is_data.get('turnover')
        existing_tags = alpha_info.get("tags", []) # 获取现有标签防止重复
        
        if check_result == "SUCCESS":
            print(f"[线程 {thread_id}] ✓ Alpha {alpha_id} 检查通过！")

            # --- PERFECT 标签逻辑 ---
            is_perfect = (sharpe > 2.0 and fitness > 1.5) and (turnover < 0.2)
            
            if is_perfect:
                print(f"[线程 {thread_id}] ✨ 出现优秀指标，正在打上 PERFECT 标签...")
                p_resp, sess = set_alpha_properties(
                    sess, 
                    alpha_id, 
                    tags="PERFECT",
                    regular_desc=f"Simulated and checked on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
            )
                if p_resp and p_resp.status_code in (200, 201):
                    print(f"[线程 {thread_id}] ✓ 成功为Alpha {alpha_id} 打上 PERFECT 标签")
                else:
                    print(f"[线程 {thread_id}] 警告: 完美标签打标失败，状态码: {p_resp.status_code if p_resp else 'None'}")
            
            else:
                print(f"[线程 {thread_id}] 正在打上SUCCESS标签...")
                response, sess = set_alpha_properties(
                    sess, 
                    alpha_id, 
                    name=datetime.now().strftime("%Y.%m.%d"),
                    tags="SUCCESS",
                    regular_desc=f"Simulated and checked on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
                )
                if response and response.status_code in (200, 201):
                    print(f"[线程 {thread_id}] ✓ 成功为Alpha {alpha_id} 打上SUCCESS标签")
                else:
                    print(f"[线程 {thread_id}] 警告: 打标签可能失败，状态码: {response.status_code if response else 'None'}")
                
            return True, alpha_id, check_result, row_index
        else:
            print(f"[线程 {thread_id}] ✗ Alpha {alpha_id} 检查未通过")
            print(f"[线程 {thread_id}] 检查结果: {check_result}")
            
            # --- POTENTIAL_SUCCESS 标签逻辑 ---
            # 检查指标是否符合潜力标准且未被标记过
            is_promising = (sharpe > 1.1 and fitness > 0.8) or (sharpe > 1.5)
            already_tagged = "SUCCESS" in existing_tags or "POTENTIAL" in existing_tags
            
            if is_promising and not already_tagged:
                print(f"[线程 {thread_id}] ✨ 发现潜力指标，正在打上 POTENTIAL 标签...")
                p_resp, sess = set_alpha_properties(
                    sess, 
                    alpha_id, 
                    tags="POTENTIAL",
                    regular_desc=f"Potential candidate: Sharpe={sharpe}, Fitness={fitness}"
                )
                if p_resp and p_resp.status_code in (200, 201):
                    print(f"[线程 {thread_id}] ✓ 成功打上 POTENTIAL 标签")
                else:
                    print(f"[线程 {thread_id}] 警告: 潜力标签打标失败，状态码: {p_resp.status_code if p_resp else 'None'}")
    
            return False, alpha_id, check_result, row_index
            
    except Exception as e:
        print(f"[线程 {thread_id}] 处理Alpha时发生错误: {e}")
        import traceback
        traceback.print_exc()
        return False, None, f"Exception: {str(e)}", row_index


def main():
    """主函数：从CSV读取alpha列表并并发批量处理"""
    print("=" * 80)
    print("Alpha批量仿真和回测脚本（从CSV读取）- 并发版本")
    print(f"开始时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 80)
    
    # 并发数量（可以在命令行参数中指定，默认3）
    max_workers = 3
    if len(sys.argv) > 1:
        try:
            max_workers = int(sys.argv[1])
        except ValueError:
            print(f"警告: 无效的并发数参数 '{sys.argv[1]}'，使用默认值3")
    
    print(f"并发数量: {max_workers}")
    
    # CSV文件路径
    csv_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 
                           './MyQuantCode/alpha_list_pending_simulated.csv')
    
    if not os.path.exists(csv_path):
        print(f"错误: CSV文件不存在: {csv_path}")
        return
    
    # 加载alpha列表
    print(f"\n加载Alpha列表: {csv_path}")
    df = load_alpha_list_from_csv(csv_path)
    print(f"总共 {len(df)} 个Alpha")
    
    # 统计状态
    status_counts = df['status'].value_counts()
    print("\n当前状态统计:")
    for status, count in status_counts.items():
        print(f"  {status}: {count}")
    
    # 筛选出待处理的alpha（状态为PENDING或空）
    pending_mask = (df['status'] == 'PENDING') | (df['status'].isna()) | (df['status'] == '')
    pending_df = df[pending_mask].copy()
    
    if len(pending_df) == 0:
        print("\n没有待处理的Alpha，程序退出")
        return
    
    print(f"\n待处理的Alpha数量: {len(pending_df)}")
    
    # 准备任务列表
    tasks = []
    for idx, (row_index, alpha_row) in enumerate(pending_df.iterrows()):
        tasks.append((alpha_row, row_index, idx, len(pending_df)))
    
    # 使用线程池并发处理
    success_count = 0
    fail_count = 0
    completed_count = 0
    
    print(f"\n开始并发处理（最多{max_workers}个并发）...")
    
    try:
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # 提交所有任务
            future_to_task = {
                executor.submit(process_single_alpha, alpha_row, row_index, idx, total, df, csv_path): 
                (alpha_row, row_index, idx, total)
                for alpha_row, row_index, idx, total in tasks
            }
            
            # 处理完成的任务
            for future in as_completed(future_to_task):
                completed_count += 1
                try:
                    success, alpha_id, check_result, row_index = future.result()
                    
                    # 更新DataFrame中的状态（使用锁保护）
                    with csv_lock:
                        if success:
                            df.at[row_index, 'status'] = 'SUCCESS'
                            df.at[row_index, 'alpha_id'] = str(alpha_id) if alpha_id else ''
                            df.at[row_index, 'check_result'] = check_result
                            df.at[row_index, 'completed_time'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                            success_count += 1
                        else:
                            df.at[row_index, 'status'] = 'FAILED'
                            df.at[row_index, 'alpha_id'] = str(alpha_id) if alpha_id else ''
                            df.at[row_index, 'check_result'] = check_result
                            df.at[row_index, 'completed_time'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                            fail_count += 1
                        
                        # 每完成一个任务就保存一次CSV（防止中断丢失进度）
                        # 注意：这里已经持有锁，所以不需要再次加锁
                        save_alpha_list_to_csv(df, csv_path, use_lock=False)
                    
                    print(f"\n[进度] 已完成 {completed_count}/{len(tasks)} 个Alpha (成功: {success_count}, 失败: {fail_count})")
                    
                except Exception as e:
                    print(f"\n处理任务时发生错误: {e}")
                    import traceback
                    traceback.print_exc()
                    fail_count += 1
                    
    except KeyboardInterrupt:
        print("\n\n用户中断，保存当前进度...")
        with csv_lock:
            save_alpha_list_to_csv(df, csv_path, use_lock=False)
        print("进度已保存，程序退出")
        return
    
    # 最终统计
    print("\n" + "=" * 80)
    print("处理完成！")
    print("=" * 80)
    print(f"成功: {success_count}")
    print(f"失败: {fail_count}")
    print(f"完成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 80)


if __name__ == "__main__":
    # 支持命令行参数指定并发数
    # 用法: python alpha_batch_simulate_from_csv.py [并发数]
    # 例如: python alpha_batch_simulate_from_csv.py 3
    main()
