from io import StringIO
import re
import pandas as pd

import functools
import numpy as np


def make_depth(df, id_col = 'id', parent_col='parent_id'):
    """
    Usage :
    df['depth'] = df['parent_id'].apply(make_depth(df))
    """

    idmap = dict(zip(df[id_col], df[parent_col]))
   
    @functools.lru_cache()
    def depth(id_):
        if np.isnan(id_):
            return 1
        return depth(idmap[id_]) + 1
    return depth

class Table:
    """
    Object holding multiple dataframes about a table
    - Currently French is not supported 
    - Notes from the metadata are not parsed
    - The whole data is present in two dataframes so it could take a lot of memory
    ...

    Attributes
    ----------
    title : title of the table
    dimensions : dimensions and name
    merged_dataframe : DataFrame with dimensions, parents, depth 
    dataframe : DataFrame holding the raw data
    metadata_frame : DataFrame holding the meta data
    

    Methods
    -------
    get_dimension_name : returns the name of a dimension
    """

    def __init__(self, table, path='dataset_download/'):
        self._metadata = self.get_metadata(f'{path}{table}_metadata.csv')
        self.meta_dataframe = self.make_meta_dataframes(self._metadata)
        self.dataframe = pd.read_csv(f'{path}{table}.csv', low_memory=False)
        self.lowercase_cols()
        self.split_coordinate()
        self.get_dimensions()
        self.merge_metadata()

    @property 
    def title(self):
        return self.meta_dataframe['header'].iloc[0]['Cube Title']
    
    @property 
    def dimensions(self):
        return self.meta_dataframe['dimensions'][['Dimension ID','Dimension name']]

    def __repr__(self):
        return f'Table : {self.title}'

    def get_metadata(self, filename):
        with open(filename,'rb') as f:
            filedata = f.read().decode('utf-8-sig')
        return filedata

    def convert_csv_to_dataframe(self, table):    
        return pd.read_csv(StringIO(table), sep=',',index_col=False, quotechar='"', quoting=1)

    def split_metadata_sections(self, metadata):
        metadata_tables = metadata.split('\n\n')
        return metadata_tables

    def make_meta_dataframes(self, metadata):
        tables = self.split_metadata_sections(metadata)
        header_df = self.convert_csv_to_dataframe(tables[0])
        dimensions_df = self.convert_csv_to_dataframe(tables[1])
        metadata_df = self.convert_csv_to_dataframe(tables[2])
        relationship_df = metadata_df[['Dimension ID','Member ID','Parent Member ID']]
        return {'header' : header_df, 
                'dimensions' : dimensions_df, 
                'metadata' : metadata_df, 
                'relationship' : relationship_df
               }
        
    def get_dimension_name(self, dim):
        return self.meta_dataframe['dimensions'].loc[self.meta_dataframe['dimensions']['Dimension ID']==dim]['Dimension name'].item()

    def lowercase_cols(self):
        self.dataframe.columns = self.dataframe.columns.str.lower()        
   
    def get_dimensions(self):
        self.nbr_dims = len(self.dataframe.iloc[0]['coordinate'].split('.'))

    def split_coordinate(self):
        """ Create dim columns based on the coordinates"""
        df = self.dataframe
        try:
            df = df.join(df['coordinate'].str.split('.', expand=True).astype('int16').rename(lambda x:'dim_{}'.format(x+1), axis=1)) 
            #df = df.join(df['coordinate'].str.split('.', expand=True).rename(lambda x:'dim_{}'.format(x+1), axis=1)) #.add_suffix('_ID'))
            """
            the line above is the same as :
            coord_df = df['coordinate'].str.split('.', expand=True)
            coord_df = coord_df.astype('int16')
            coord_df.rename(lambda x:'dim_{}'.format(x+1), axis=1, inplace=True)
            df = df.join(coord_df)
            """
        except Exception as e:
            print('Error : ',e)
        self.dataframe = df

    def merge_metadata(self):
        relationship_df = self.meta_dataframe['relationship']
        df = self.dataframe
        for dim in range(1,self.nbr_dims+1):
            col = 'dim_{}'.format(dim)
            # select dimension and add prefix to prevent column name clash
            dimension_df = relationship_df[relationship_df['Dimension ID'] == dim].add_prefix(f'{col}_') 

            dimension_df[f"{col}_depth"] = dimension_df[f'{col}_Parent Member ID'].apply(make_depth(dimension_df, id_col=f'{col}_Member ID', parent_col=f'{col}_Parent Member ID'))
            df = pd.merge(
                                 left=df,
                                 right=dimension_df, 
                                left_on=col, 
                                right_on=f'{col}_Member ID',
                                )
        self.merged_dataframe = df  
