# statscantools
Tools for fetching and converting Statistics Canada to Pandas DataFrame

There is a jupyter notebook to show how to fetch the catalog of tables wds_catalog.ipynb

There are 2 examples example_car and example_price_index on usage

**Most of the date from Statistics Canada includes clubed data so choosing the "depth" for each dimension is essential to avoid aggregated date to show in the dataframe.**

- Currently French is not supported 
- Notes from the metadata are not parsed
- The whole data is present in two dataframes so it could take a lot of memory, once you got the dataframe you want you can free memory by deleting the table object.
- The data is cached in the dataset_download to avoid redownloading the data eachtime you need to reload the tables.

# Basic usage :

    from statscantools.table import Table
    from statscantools.wds import wds_fetch_table

    wds_fetch_table('20100024')
    table = Table('20100024')
    df = table.merged_dataframe


Each row will contain relationship information:
- dim_#_Member ID	
- dim_#_Parent Member ID	
- dim_#_depth

