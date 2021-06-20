import os, openpyxl as opxl, pandas as pd, numpy as np


def Write_DF_to_WS(wsTarget, dfSoruce, numRowOffSet=0, numColOffSet=0, isIndexWrite=False, replaceNaN=''):
    lenDFRow, lenDFCol = dfSoruce.shape
    if isIndexWrite:
        lenDFCol += 1   # accommodating index column
    dfObj = dfSoruce.fillna(replaceNaN)
    lenWSRow = lenDFRow + numRowOffSet
    lenWSCol = lenDFCol + numColOffSet
    wsTarget.cell(row=lenWSRow, column=lenWSCol).value = None   # allocate memory
    iterWSrows = wsTarget.iter_rows()
    iterDFrows = dfObj.itertuples(name=None, index=isIndexWrite)
    for _ in range(numRowOffSet):
        _ = next(iterWSrows)
    for seqRow in range(lenDFRow):
        rowWS = next(iterWSrows)
        rowDF = next(iterDFrows)
        for seqCol in range(lenDFCol):
            rowWS[seqCol + numColOffSet].value = rowDF[seqCol]


if __name__ == '__main__':
    arrPath = ['..','01-Test']
    strTestWB = 'emptyWB.xlsx'
    wbTest= opxl.load_workbook(filename=os.path.join(*arrPath, strTestWB))
    wsS1 = wbTest.worksheets[0]

    dfTest = pd.DataFrame([[1,2,3],[4,5,np.nan]],index=['a','b'],columns=[11,22,33])
    Write_DF_to_WS(wsS1,dfTest,numRowOffSet=1,numColOffSet=1, isIndexWrite=False)
    wbTest.save(os.path.join(*arrPath, strTestWB))

