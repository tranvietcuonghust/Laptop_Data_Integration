
import pandas as pd
import fuzzymatcher
import sys


thinkpro = pd.read_csv('/home/tranvietcuong/Desktop/Tich_hop_du_lieu/group-1-it5420/Spark/resources/data/cleaned_laptop_thinkpro.csv')
techcare = pd.read_csv('/home/tranvietcuong/Desktop/Tich_hop_du_lieu/group-1-it5420/Spark/resources/data/cleaned_laptop_techcare.csv')
laptop88 = pd.read_csv('/home/tranvietcuong/Desktop/Tich_hop_du_lieu/group-1-it5420/Spark/resources/data/cleaned_laptop_laptop88.csv')
phucanh = pd.read_csv('/home/tranvietcuong/Desktop/Tich_hop_du_lieu/group-1-it5420/Spark/resources/data/cleaned_laptop_phucanh.csv')
tgdd     = pd.read_csv('/home/tranvietcuong/Desktop/Tich_hop_du_lieu/group-1-it5420/Spark/resources/data/cleaned_laptop_tgdd.csv')
trungtran = pd.read_csv('/home/tranvietcuong/Desktop/Tich_hop_du_lieu/group-1-it5420/Spark/resources/data/cleaned_laptop_trungtran.csv')

mediated_schema = ['Product', 'Price', 'Image', 'Link', 'CPU', 'RAM', 'Storage', 'Graphics', 'Screen',
                  'Status', 'Color', 'Weight', 'Battery', 'OS', 'Brand', 'Source']

laptop88.rename(columns={'Name': 'Product','URL':'Link','ImgURL':'Image','Display':'Screen','Ram':'RAM'}, inplace=True)
phucanh.rename(columns={'Name': 'Product','URL':'Link','ImgURL':'Image','Display':'Screen','Ram':'RAM'}, inplace=True)
trungtran.rename(columns={'Name': 'Product','URL':'Link','ImgURL':'Image','Display':'Screen','Ram':'RAM'}, inplace=True)
techcare.rename(columns={'Ram':'RAM'}, inplace=True)

med_laptop88 = pd.DataFrame(columns=mediated_schema)
med_techcare = pd.DataFrame(columns=mediated_schema)
med_phucanh = pd.DataFrame(columns=mediated_schema)
med_trungtran = pd.DataFrame(columns=mediated_schema)
med_thinkpro = pd.DataFrame(columns=mediated_schema)
med_tgdd = pd.DataFrame(columns=mediated_schema)

for column in mediated_schema:
    if column in laptop88.columns:
        med_laptop88[column] = laptop88[column]
    if column in techcare.columns:
        med_techcare[column] = techcare[column]
    if column in phucanh.columns:
        med_phucanh[column] = phucanh[column]
    if column in trungtran.columns:
        med_trungtran[column] = trungtran[column]
    if column in thinkpro.columns:
        med_thinkpro[column] = thinkpro[column]
    if column in tgdd.columns:
        med_tgdd[column] = tgdd[column]

# Set the 'Source' column to the name of the original DataFrame
med_laptop88['Source'] = 'laptop88'
med_techcare['Source'] = 'techcare'
med_phucanh['Source'] = 'phucanh'
med_trungtran['Source'] = 'trungtran'
med_thinkpro['Source'] = 'thinkpro'
med_tgdd['Source'] = 'tgdd'

fuzzy_matched = fuzzymatcher.fuzzy_left_join(med_laptop88, med_phucanh, ['Product', 'CPU', 'RAM', 'Storage', 'Graphics', 'Screen'], ['Product', 'CPU', 'RAM', 'Storage', 'Graphics', 'Screen'])

# Step 5: Specify the similarity threshold
threshold = 0.5

# Step 6: Create a new dataframe df1_2 with assigned product groups
df1_2 = pd.concat([med_laptop88, med_phucanh])  # Combine df1 and df2 into a single dataframe
df1_2['Product_Group'] = df1_2['Product']  # Initialize Product_Group column with Product values

for _, row in fuzzy_matched.iterrows():
    if row['best_match_score'] >= 0.1539:
        product = row['Product_right']
        group = row['Product_left']
        df1_2.loc[df1_2['Product'] == product, 'Product_Group'] = group

# Step 7: Remove records with None values in Product_Group
df1_2 = df1_2.dropna(subset=['Product_Group'])

# Step 8: Reorder the columns of df1_2 to match the desired schema
df1_2 = df1_2[['Product_Group', 'Product', 'Price', 'Image', 'Link', 'CPU', 'RAM', 'Storage', 'Graphics', 'Screen', 'Status', 'Color', 'Weight', 'Battery', 'OS', 'Brand', 'Source']]


med_techcare['Product_Group'] = med_techcare['Product']
fuzzy_matched_2_3 = fuzzymatcher.fuzzy_left_join(df1_2, med_techcare, ['Product', 'CPU', 'RAM', 'Storage', 'Graphics', 'Screen'], ['Product', 'CPU', 'RAM', 'Storage', 'Graphics', 'Screen'])
df1_2_3 = pd.concat([df1_2, med_techcare])  # Combine df1_2 and df3 into a single dataframe

for _, row in fuzzy_matched_2_3.iterrows():
    if row['best_match_score'] >= 0.462:
        product = row['Product_right']
        group = row['Product_Group_left']
        df1_2_3.loc[df1_2_3['Product'] == product, 'Product_Group'] = group
df1_2_3 = df1_2_3[['Product_Group', 'Product', 'Price', 'Image', 'Link', 'CPU', 'RAM', 'Storage', 'Graphics', 'Screen', 'Status', 'Color', 'Weight', 'Battery', 'OS', 'Brand', 'Source']]

med_trungtran['Product_Group'] = med_trungtran['Product']
fuzzy_matched_2_3_4 = fuzzymatcher.fuzzy_left_join(df1_2_3, med_trungtran, ['Product', 'CPU', 'RAM', 'Storage', 'Graphics', 'Screen'], ['Product', 'CPU', 'RAM', 'Storage', 'Graphics', 'Screen'])
df1_2_3_4 = pd.concat([df1_2_3, med_trungtran])  # Combine df1_2 and df3 into a single dataframe

for _, row in fuzzy_matched_2_3_4.iterrows():
    if row['best_match_score'] >= threshold:
        product = row['Product_right']
        group = row['Product_Group_left']
        df1_2_3_4.loc[df1_2_3_4['Product'] == product, 'Product_Group'] = group
df1_2_3_4= df1_2_3_4[['Product_Group', 'Product', 'Price', 'Image', 'Link', 'CPU', 'RAM', 'Storage', 'Graphics', 'Screen', 'Status', 'Color', 'Weight', 'Battery', 'OS', 'Brand', 'Source']]

med_thinkpro['Product_Group'] = med_thinkpro['Product']
fuzzy_matched_2_3_4_5 = fuzzymatcher.fuzzy_left_join(df1_2_3_4, med_thinkpro, ['Product', 'CPU', 'RAM', 'Storage', 'Graphics', 'Screen'], ['Product', 'CPU', 'RAM', 'Storage', 'Graphics', 'Screen'])
df1_2_3_4_5 = pd.concat([df1_2_3_4, med_thinkpro])  # Combine df1_2 and df3 into a single dataframe

for _, row in fuzzy_matched_2_3_4_5.iterrows():
    if row['best_match_score'] >= 0.142:
        product = row['Product_right']
        group = row['Product_Group_left']
        df1_2_3_4_5.loc[df1_2_3_4_5['Product'] == product, 'Product_Group'] = group
df1_2_3_4_5= df1_2_3_4_5[['Product_Group', 'Product', 'Price', 'Image', 'Link', 'CPU', 'RAM', 'Storage', 'Graphics', 'Screen', 'Status', 'Color', 'Weight', 'Battery', 'OS', 'Brand', 'Source']]

med_tgdd['Product_Group'] = med_tgdd['Product']
fuzzy_matched_2_3_4_5_6 = fuzzymatcher.fuzzy_left_join(df1_2_3_4_5, med_tgdd, ['Product', 'CPU', 'RAM', 'Storage', 'Graphics', 'Screen'], ['Product', 'CPU', 'RAM', 'Storage', 'Graphics', 'Screen'])
df1_2_3_4_5_6 = pd.concat([df1_2_3_4_5, med_tgdd])  # Combine df1_2 and df3 into a single dataframe

for _, row in fuzzy_matched_2_3_4_5_6.iterrows():
    if row['best_match_score'] >= 0.373:
        product = row['Product_right']
        group = row['Product_Group_left']
        df1_2_3_4_5_6.loc[df1_2_3_4_5_6['Product'] == product, 'Product_Group'] = group
df1_2_3_4_5_6= df1_2_3_4_5_6[['Product_Group', 'Product', 'Price', 'Image', 'Link', 'CPU', 'RAM', 'Storage', 'Graphics', 'Screen', 'Status', 'Color', 'Weight', 'Battery', 'OS', 'Brand', 'Source']]

grouped = df1_2_3_4_5_6.groupby("Product_Group")

# Step 3-6: Create the merged rows and collect source, link, and price information
merged_rows = []

for group_name, group_data in grouped:
    merged_row = {
        "Product_Group": group_name,
        "Image": group_data["Image"].dropna().iloc[0] if not group_data["Image"].isnull().all() else pd.NA,
        "CPU": group_data["CPU"].dropna().iloc[0] if not group_data["CPU"].isnull().all() else pd.NA,
        "RAM": group_data["RAM"].dropna().iloc[0] if not group_data["RAM"].isnull().all() else pd.NA,
        "Storage": group_data["Storage"].dropna().iloc[0] if not group_data["Storage"].isnull().all() else pd.NA,
        "Graphics": group_data["Graphics"].dropna().iloc[0] if not group_data["Graphics"].isnull().all() else pd.NA,
        "Screen": group_data["Screen"].dropna().iloc[0] if not group_data["Screen"].isnull().all() else pd.NA,
        "Status": group_data["Status"].dropna().iloc[0] if not group_data["Status"].isnull().all() else pd.NA,
        "Color": group_data["Color"].dropna().iloc[0] if not group_data["Color"].isnull().all() else pd.NA,
        "Weight": group_data["Weight"].dropna().iloc[0] if not group_data["Weight"].isnull().all() else pd.NA,
        "Battery": group_data["Battery"].dropna().iloc[0] if not group_data["Battery"].isnull().all() else pd.NA,
        "OS": group_data["OS"].dropna().iloc[0] if not group_data["OS"].isnull().all() else pd.NA,
        "Brand": group_data["Brand"].dropna().iloc[0] if not group_data["Brand"].isnull().all() else pd.NA,
    }

    source_data = {}
    for _, row in group_data.iterrows():
        source = row["Source"]
        link = row["Link"]
        price = row["Price"]
        if pd.notnull(source) and pd.notnull(link) and pd.notnull(price):
            if source not in source_data:
                source_data[source] = {"link": set([link]), "price": set([row["Price"]]), "status": set([row["Status"]])}
            else:
                source_data[source]["link"].add(link)
                source_data[source]["status"].add(row["Status"])
                source_data[source]["price"].add(row["Price"])
    merged_row["Originals"] = source_data
    merged_rows.append(merged_row)

# Step 7: Assign the new schema to the DataFrame
new_columns = ["Product_Group", "Image", "CPU", "RAM", "Storage", "Graphics", "Screen", "Status", "Color", "Weight", "Battery", "OS", "Brand", "Originals"]
df_merged = pd.DataFrame(merged_rows, columns=new_columns)
df_merged.to_csv('/home/tranvietcuong/Desktop/Tich_hop_du_lieu/group-1-it5420/Spark/resources/data/all_laptop.csv')
sys.exit(0)