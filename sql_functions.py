import psycopg2 as ps
import pandas as pd

def connect_to_db(host_name,username,passwd,db_name,port_con):
    '''
    Function: Connect a host to a database in this case the an AWS database
    
    Parameters: Host_name is the address you use to connect to the database, 
                username is the name used in the service database,
                passwd is your passsword,
                db_name is the name of the database,
                port_cont is the port used to communicate to the database.
                
    return: If the connection is made, return the conn variable
    
    '''
    try:
        conn = ps.connect(host = host_name, database = db_name, user = username, password = passwd, port=port_con )
    except ps.OperationalError as oe:
        raise oe
    else:
        print('connected')
    return  conn


def create_table(curr):
    '''
    Function: Create a sql table to be put in the data base if the table doesn't exists already
    
    Parameters: curr is the current variable, that will be used to perform the sql commands
    
    This function doesn't return anything it just execute the create table sql command
    '''
    create_table_command = (""" CREATE TABLE IF NOT EXISTS monster_table(
                            name  VARCHAR(255) NOT NULL PRIMARY KEY,
                            size VARCHAR(255) NOT NULL,
                            type VARCHAR NOT NULL,
                            subtype VARCHAR(255),
                            alignment VARCHAR(255),
                            armor_class INTEGER NOT NULL,
                            hit_points INTEGER NOT NULL,
                            str_stats INTEGER NOT NULL,
                            dex_stats INTEGER NOT NULL,
                            con_stats INTEGER NOT NULL,
                            int_stats INTEGER NOT NULL,
                            wis_stats INTEGER NOT NULL,
                            cha_stats INTEGER NOT NULL,
                            dmg_vul TEXT,
                            dmg_resist TEXT,
                            dmg_immun TEXT,
                            condition_immun TEXT,
                            cr FLOAT NOT NULL,
                            legendary_desc TEXT,
                            size_m FLOAT NOT NULL
                            )

    """)
    
    curr.execute(create_table_command)
    
    
    
def check_if_monster_exist(curr, name):
    '''
    Function: It will check existence of a monster in the data that are already on the table. In this case if the monster
    already exist or not 
    
    Parameters: curr the variable use to perform the sql commands,
                name is the name of the monster to check in this case
                
    Return: If the monster didn't exist it will return empty
    
    '''
    query = ("""SELECT name FROM monster_table WHERE name = %s""")
    curr.execute(query, (name,))
    
    return curr.fetchone() is not None


def update_row(curr, name, size, types, subtype, alignment, armor_class, hit_points, str_stats,
               dex_stats, con_stats, int_stats, wis_stats, cha_stats, dmg_vul, dmg_resist,
               dmg_immun, condition_immun, cr, legendary_desc, size_m):
    '''
    Function: It will update the table data 
    
    Parameters: curr the variable use to perform the sql commands,
                columns of the dataframe
                
    Return: The function does not return anything it just execute the query
    
    '''
    
    
    query = (""" UPDATE  monster_table
                SET size = %s,
                    type = %s,
                    subtype = %s,
                    alignment = %s,
                    armor_class = %s,
                    hit_points  = %s,
                    str_stats = %s,
                    dex_stats = %s,
                    con_stats = %s,
                    int_stats = %s,
                    wis_stats = %s,
                    cha_stats = %s,
                    dmg_vul = %s,
                    dmg_resist = %s,
                    dmg_immun = %s,
                    condition_immun = %s,
                    cr FLOAT = %s,
                    legendary_desc  = %s
                    size_m = %s
                    WHERE name = %s;
    """)
    
    vars_to_update = (name, size, types, subtype, alignment, armor_class,
       hit_points, str_stats, dex_stats, con_stats, int_stats,
       wis_stats, cha_stats, dmg_vul, dmg_resist, dmg_immun,
       condition_immun, cr, legendary_desc, size_m)
    
    curr.execute(query, vars_to_update)
    
    
    
def  update_db(curr,df):
    '''
    Function: Create a temporary dataframe to check if a monster already exists in the table. In this case if the monster
    already exist it will update the row with new information if any. Or if the monster doesn't exist will the append
    the new monster in the table.
    
    Obs: The Function also deals with scalability, it will upload one row at a time, not the entire data all at once,
         this is important when deal with loads of information, so we don't have memory or performance issues
    
    Parameters: curr variable use to perform sql commands
                df the dataframe to be checked
                
    Return: The temporary dataframe that has the new information
    
    '''
    
    temp_df = pd.DataFrame(columns = ['name', 'size', 'types', 'subtype', 'alignment', 'armor_class',
           'hit_points', 'str_stats', 'dex_stats', 'con_stats', 'int_stats',
           'wis_stats', 'cha_stats', 'dmg_vul', 'dmg_resist', 'dmg_immun',
           'condition_immun', 'cr', 'legendary_desc', 'size_m'])

    for i, row in df.iterrows():
        if check_if_monster_exist(curr, row['name']): #if monster exists it just update the table in case anything change
            update_row(curr, row['name'], row['size'], row['types'],row['subtype'], row['alignment'], row['armor_class'],
           row['hit_points'], row['str_stats'], row['dex_stats'],row[ 'con_stats'], row['int_stats'],
           row['wis_stats'], row['cha_stats'], row['dmg_vul'], row['dmg_resist'],row[ 'dmg_immun'],
           row['condition_immun'],row['cr'], row['legendary_desc'], row['size_m'])
        else:
            temp_df = temp_df.append(row)
    return temp_df



def insert_into_table(curr, name, size, types, subtype, alignment, armor_class, hit_points, str_stats,
               dex_stats, con_stats, int_stats, wis_stats, cha_stats, dmg_vul, dmg_resist,
               dmg_immun, condition_immun, cr, legendary_desc, size_m ):
    '''
     Function: Insert the new information on the temporary dataframe in the table and then upload to the database
    
     Parameters: curr variable used to perform sql commands
                columns of the dataframe that will be uploaded
                
     Return: The function does not return anything it just execute the query
    
    
    '''
    #create insert command
    insert_into_monstertable = ("""INSERT INTO monster_table (name, size, type, subtype, alignment, armor_class,
           hit_points, str_stats, dex_stats, con_stats, int_stats,
           wis_stats, cha_stats, dmg_vul, dmg_resist, dmg_immun,
           condition_immun, cr, legendary_desc, size_m)
           VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);
    """)

    row_to_insert = (name, size, types, subtype, alignment, armor_class,
           hit_points, str_stats, dex_stats, con_stats, int_stats,
           wis_stats, cha_stats, dmg_vul, dmg_resist, dmg_immun,
           condition_immun, cr, legendary_desc, size_m)

    curr.execute(insert_into_monstertable, row_to_insert)


def append_to_db(curr,df ):
    '''
    Function: Append the new data into the table
    
     Obs: The Function also deals with scalability, it will upload one row at a time, not the entire data all at once,
         this is important when deal with loads of information, so we don't have memory or performance issues
         
    Parameters: curr  variable used to perform sql commands
                df the dataframe that has the new information to be appended
    
    Return: The function does not return anything it just calls the insert into table function and updates the rows
    
    '''
    
    for i, row in df.iterrows():
        insert_into_table(curr, row['name'], row['size'], row['types'],row['subtype'], row['alignment'], row['armor_class'],
               row['hit_points'], row['str_stats'], row['dex_stats'],row[ 'con_stats'], row['int_stats'],
               row['wis_stats'], row['cha_stats'], row['dmg_vul'], row['dmg_resist'],row[ 'dmg_immun'],
               row['condition_immun'],row['cr'], row['legendary_desc'], row['size_m'])