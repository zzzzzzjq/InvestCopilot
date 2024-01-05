__author__ = 'liuy'

import pandas as pd
import InvestCopilot_App.models.toolsutils.connect as conn


def ExportExcelfromDf(title,inputdata):
    writer = pd.ExcelWriter(title)
    pd.DataFrame(inputdata).to_excel(writer)
    writer.close()