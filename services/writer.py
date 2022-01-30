from docx import Document
from docx.shared import Inches, Cm, Pt
from docx.enum.table import WD_TABLE_ALIGNMENT
from docx.enum.text import WD_ALIGN_PARAGRAPH

from docx2pdf import convert

import pandas as pd
import numpy as np
import pickle

import os



def df2table(doc,df,fontsize = 7.5):
    rows, cols = df.shape
    table = doc.add_table(rows = rows, cols = cols, style = "Table Grid")
    table.autofit = False

    # config table width
    width = 19.5
    weights = [1]*cols
    weights[0] = 3
    weights = np.array(weights)
    weights =  width*(weights/sum(weights))
    
    
    for col in range(cols):
        for cell in table.columns[col].cells:
            cell.width = Cm(weights[col])
    
    # insert values
    for r in range(0,rows):
        row_cells = table.rows[r].cells
        for c in range(cols):
            row_cells[c].text = str(df.iloc[r,c])

            
            paragraphs = row_cells[c].paragraphs
            for paragraph in paragraphs:
                for run in paragraph.runs:
                    font = run.font
                    font.size = Pt(fontsize)
                paragraph.alignment=WD_ALIGN_PARAGRAPH.CENTER
    return




def add_header(df):
    df.loc[-1] = list(df.columns)
    df.index = df.index + 1
    df = df.sort_index()
    return df

def to_pdf(doc):
    convert(doc)

def load_pickle(filepath):
    with open(filepath, "rb") as file:
        return pickle.load(file)
    
def df_filter(df, items):
    return df[[True if item in items else False for item in list(df.iloc[:,0])]]