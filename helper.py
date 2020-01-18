with open('sql.txt', 'r') as rf:
    for line in rf:
        line = line.strip()
        str_temp = f"'{line}': result['{line.title()}'], "  # 'title': result['Title']}
        print(str_temp)
