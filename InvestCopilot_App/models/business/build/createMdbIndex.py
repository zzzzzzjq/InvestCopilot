


from InvestCopilot_App.models.toolsutils import dbmongo
def stockChat_unique():
    with dbmongo.Mongo("gptchat") as md:
        mydb = md.db
        conversationsSet = mydb["stockchat_conv"]
        conversationsSet.drop()
        # 在id字段上创建唯一索引
        conversationsSet.create_index("conversation_id", unique=True)

if __name__ == '__main__':
    stockChat_unique()