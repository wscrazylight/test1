#include <stdio.h>
#include <stdlib.h>
#include <mysql.h>
#include <string.h>

#define base_1KB (1024)

bool mysqlOpen(MYSQL *mydata, MYSQL **handle)
{
    mysql_library_init(0,NULL,NULL);                    //初始化MySQL C API库
    mysql_init(mydata);                                 //初始化mydata的数据结构，mydata是MYSQL对象
    mysql_options(mydata,MYSQL_SET_CHARSET_NAME,"utf8");//连接选项：添加GBK/UTF-8字符集支持
    char value = 1;
    mysql_options(mydata,MYSQL_OPT_RECONNECT,&value);   //开启自动重连
    printf("begin to connect database\n");

    if(!(*handle = mysql_real_connect(mydata,"172.24.1.200","root","","zn_database",3306,0,0)))
    {
        printf("Couldn't connect to database : %s!\n",mysql_error(mydata));
        return false;
    }
    printf("connect database success\n");

    return true;
}

void mysqlClose(MYSQL *handle)
{
    if (NULL != handle)
    {
        mysql_close(handle);        //关闭数据库连接
    }
}

int keyword_insertMySql(MYSQL *handle, const char *table,
                                       const char *unit1, const char *keyword,
                                       const char *unit2, const char *group)
{
    char sql[1024];
    sprintf(sql, "INSERT INTO `%s` (`%s`, `%s`) VALUES ('%s','%s')"
                 , table, unit1, unit2, keyword, group);
    if(!mysql_query(handle, sql))
    {
        return mysql_affected_rows(handle);
    }

    return 0;
}

int main()
{
    MYSQL mydata,*handle;//定义数据库连接的句柄，它被用于几乎所有的MySQL函数
    if (!mysqlOpen(&mydata, &handle))
    {
        return -1;
    }

    MYSQL_RES *result;                  //查询结果集，结构类型
    MYSQL_ROW row;                      //存放一行查询结果的字符串数组

    char  querysql[160] = "select `name`,`group`,`dokeyword`,`dodomain` from `table_keywords_to_add`;";

    if(mysql_query(handle,querysql))    //查询数据库
    {
        printf("Query failed : %s\n",mysql_error(handle));
        return -1;
    }
    printf("Query database success\n");

    if (!(result=mysql_store_result(handle))) //存储结果集
    {
        printf("Couldn't get result from %s\n", mysql_error(handle));
        return -1;
    }
    printf("Get %d rows from database\n", (int)mysql_num_rows(result));

    int  num = 0;
    while (row = mysql_fetch_row(result))//读取结果集的内容
    {
        if ((row[0]!=NULL) && (row[0][0] != '\0')
            && (row[1]!=NULL) && (row[1][0] != '\0')
            && (row[2]!=NULL) && (row[2][0] != '\0')
            && (row[3]!=NULL) && (row[3][0] != '\0'))
        {
            printf("----%24s, \t%s, %s, %s\n", row[0], (*row[1]=='1'?"novel":(*row[1]=='2'?"game":(*row[1]=='3'?"sex":"NULL invalid"))), row[2], row[3]);

            if (*row[1]=='1')//add 小说
            {
                if (*row[2]=='1')//add dokeyword
                {
                    num += keyword_insertMySql(handle, "table_keywords_ebook_extend",
                                                       "keyword", row[0], "group", "novel");
                    num += keyword_insertMySql(handle, "x_bloom_keywords_extend",
                                                       "name", row[0], "group", "novel");
                }

                if (*row[3]=='1')//add dodomain
                {
                    num += keyword_insertMySql(handle, "table_keywords_ebook",
                                                       "keyword", row[0], "group", "novel");
                    num += keyword_insertMySql(handle, "x_bloom_keywords",
                                                       "name", row[0], "group", "novel");
                }
            }

            if (*row[1]=='2')//add 游戏
            {
                if (*row[2]=='1')//add dokeyword
                {
                    num += keyword_insertMySql(handle, "table_keywords_game_extend",
                                                       "keyword", row[0], "group", "game");
                    num += keyword_insertMySql(handle, "x_bloom_keywords_extend",
                                                       "name", row[0], "group", "game");
                }

                if (*row[3]=='1')//add dodomain
                {
                    num += keyword_insertMySql(handle, "table_keywords_game",
                                                       "keyword", row[0], "group", "game");
                    num += keyword_insertMySql(handle, "x_bloom_keywords",
                                                       "name", row[0], "group", "game");
                }
            }

            if (*row[1]=='3')//add 色情
            {
                if (*row[2]=='1')//add dokeyword
                {
                    num += keyword_insertMySql(handle, "table_keywords_sex_extend",
                                                       "keyword", row[0], "group", "sex");
                    num += keyword_insertMySql(handle, "x_bloom_keywords_extend",
                                                       "name", row[0], "group", "sex");
                }

                if (*row[3]=='1')//add dodomain
                {
                    num += keyword_insertMySql(handle, "table_keywords_sex",
                                                       "keyword", row[0], "group", "sex");
                    num += keyword_insertMySql(handle, "x_bloom_keywords",
                                                       "name", row[0], "group", "sex");
                }
            }

            if (num != 0)
            {
                printf("    add \'%d\' rows success.\n", num);
                num = 0;
            }
        }
    }
    mysql_free_result(result);  //释放结果集

    mysqlClose(handle);
}

