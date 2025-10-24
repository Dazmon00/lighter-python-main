import csv
import os

def add_account(account_index, api_key_index, api_key_private_key, description):
    """添加新账户到CSV文件"""
    csv_file = "examples/accounts.csv"
    
    # 检查文件是否存在，如果不存在则创建
    if not os.path.exists(csv_file):
        with open(csv_file, 'w', newline='', encoding='utf-8') as file:
            writer = csv.writer(file)
            writer.writerow(['account_index', 'api_key_index', 'api_key_private_key', 'description'])
    
    # 添加新账户
    with open(csv_file, 'a', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerow([account_index, api_key_index, api_key_private_key, description])
    
    print(f"成功添加账户: {description}")

def list_accounts():
    """列出所有账户"""
    csv_file = "examples/accounts.csv"
    if not os.path.exists(csv_file):
        print("没有找到账户文件")
        return
    
    print("=== 账户列表 ===")
    with open(csv_file, 'r', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        for i, row in enumerate(reader, 1):
            print(f"{i}. {row['description']} (索引: {row['account_index']}, API密钥: {row['api_key_index']})")

def remove_account(account_index):
    """删除指定账户"""
    csv_file = "examples/accounts.csv"
    if not os.path.exists(csv_file):
        print("没有找到账户文件")
        return
    
    # 读取所有账户
    accounts = []
    with open(csv_file, 'r', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        for row in reader:
            if int(row['account_index']) != account_index:
                accounts.append(row)
    
    # 写回文件
    with open(csv_file, 'w', newline='', encoding='utf-8') as file:
        if accounts:
            writer = csv.DictWriter(file, fieldnames=['account_index', 'api_key_index', 'api_key_private_key', 'description'])
            writer.writeheader()
            writer.writerows(accounts)
    
    print(f"成功删除账户索引: {account_index}")

def main():
    while True:
        print("\n=== 账户管理 ===")
        print("1. 添加账户")
        print("2. 列出账户")
        print("3. 删除账户")
        print("4. 退出")
        
        choice = input("请选择操作 (1-4): ")
        
        if choice == '1':
            account_index = int(input("请输入账户索引: "))
            api_key_index = int(input("请输入API密钥索引: "))
            api_key_private_key = input("请输入API密钥私钥: ")
            description = input("请输入账户描述: ")
            add_account(account_index, api_key_index, api_key_private_key, description)
            
        elif choice == '2':
            list_accounts()
            
        elif choice == '3':
            account_index = int(input("请输入要删除的账户索引: "))
            remove_account(account_index)
            
        elif choice == '4':
            print("退出程序")
            break
            
        else:
            print("无效选择，请重新输入")

if __name__ == "__main__":
    main()
