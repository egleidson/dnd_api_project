import pandas as pd
import requests
import seaborn as sns
import matplotlib.pyplot as plt

def monster_dnd_dataframe():
    monster_df = []

    for i in range(1,23):
    
        url = "https://api.open5e.com/monsters/?page={}".format(i)
        response = requests.get(url).json()
   
        for item in response['results']:
            data = {'name':item['name'],
                    'size': item['size'],
                    'types':item['type'],
                    'subtype': item['subtype'],
                    'alignment':item['alignment'],
                    'armor_class':item['armor_class'],
                    'hit_points':item['hit_points'],
                    'str_stats':item['strength'],
                    'dex_stats':item['dexterity'],
                    'con_stats':item['constitution'],
                    'int_stats':item['intelligence'],
                    'wis_stats':item['wisdom'],
                    'cha_stats':item['charisma'],
                    'dmg_vul':item['damage_vulnerabilities'],
                    'dmg_resist':item['damage_resistances'],
                    'dmg_immun':item['damage_immunities'],
                    'condition_immun':item['condition_immunities'],
                    'cr': item['challenge_rating'],
                    'legendary_desc':item['legendary_desc']
                    
                                
               }
            monster_df.append(data)
    
    return (pd.DataFrame (monster_df))


def hist_plot(dataframe, string1, string2='size') :   
    plt.figure(figsize=(10,8))
    sns.histplot( x = string1, hue = string2, palette='pastel', data = dataframe)
    plt.xticks(rotation =90)
    plt.show()


def bar_plot(dataframe, string1, string2, string3='size'):   
    plt.figure(figsize=(10,8))
    sns.barplot(x = string1, y = string2, hue=string3, palette='pastel', data = dataframe)
    plt.xticks(rotation =90)
    plt.show()
    
    
def violin_plot(dataframe, string1, string2, string3='size'):
    plt.figure(figsize=(10,8))
    sns.violinplot(x = string1, y = string2, hue=string3, palette='pastel', data = dataframe)
    plt.xticks(rotation =90)
    plt.show()
    
def scatter_plot(dataframe, xvalue, yvalue, string3 = 'size'):
    plt.figure(figsize=(12,8))
    sns.scatterplot(x= xvalue, y =yvalue, hue = string3, data = dataframe )
    plt.show()