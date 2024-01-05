#pip install python-magic-bin python-docx
import traceback

import magic
import docx
import pdfplumber
from InvestCopilot_App.models.toolsutils.ResultData import ResultData
import InvestCopilot_App.models.toolsutils.LoggerUtils as logger_utils
Logger = logger_utils.LoggerUtils()
class fileUtils():

    #解下word文档：
    def detect_file_type(self,file_path):
        mime = magic.Magic()
        file_type = mime.from_file(file_path)
        return file_type.lower()

    def parse_pdf(self,file_path):
        rst=ResultData()
        try:
            textList = []
            with pdfplumber.open(file_path) as pdf:
                _pages = pdf.pages
                # 读取每个页面的文本内容
                for _pageIdx in range(len(_pages)):
                    _page = _pages[_pageIdx]
                    texts = _page.extract_text()
                    textList.append(texts)
                rst.text="\n".join(textList)
                return rst
        except:
            Logger.error("parse_pdf error")
            Logger.error(traceback.format_exc())
            rst.errorData("抱歉，pdf文件解析异常，请联系管理员处理。")
            #尝试其他方式读取pdf
            try:
                textList = []
                import fitz  # pip install PyMuPDF
                with fitz.open(file_path) as pdf_document:
                    for page_number in range(pdf_document.page_count):
                        page = pdf_document[page_number]
                        # 设定页眉和页脚的高度，单位是 points
                        # header_height = 50
                        # footer_height = 50
                        # 去除页眉和页脚
                        # remove_header_footer(page, header_height, footer_height)
                        # 获取页面的文本信息
                        # 处理文本数据
                        textList.append(page.get_text())
                    rst = ResultData()
                    rst.text = "\n".join(textList)
                    return rst
            except:
                Logger.error("parse_pdf error")
                Logger.error(traceback.format_exc())
                rst.errorData("抱歉，pdf文件解析异常，请联系管理员处理。")
        return rst


    def parse_docx(self,file_path):
        rst=ResultData()
        try:
            doc = docx.Document(file_path)
            text = ""
            for paragraph in doc.paragraphs:
                text += paragraph.text + "\n"
            rst.text = text
            return rst
        except:
            Logger.error("parse_docx error")
            Logger.error(traceback.format_exc())
            #尝试其他方式读取pdf
            pass

    def parse_txt(self,file_path):
        rst=ResultData()
        try:
            with open(file_path, 'r', encoding='utf-8') as file:
                text = file.read()
            rst.text = text
            return rst
        except:
            Logger.error("parse_txt error")
            Logger.error(traceback.format_exc())
            #尝试其他方式读取pdf
            pass
    def parse_file(self,file_path):
        rst=ResultData()
        rst.text=""
        file_type = self.detect_file_type(file_path)
        # if file_type not in ['pdf','word','text']:
        #     rst.errorData(errorMsg="支持.doc,.txt,.pdf格式文件")
        #     return rst
        if 'pdf' in file_type:
            return self.parse_pdf(file_path)
        elif 'word' in file_type or 'officedocument.wordprocessingml' in file_type or 'microsoft ooxml' in file_type:
            return self.parse_docx(file_path)
        elif 'text' in file_type or 'plain' in file_type:
            return self.parse_txt(file_path)
        else:
            rst.errorData(errorMsg="支持.doc,.txt,.pdf,.md格式文件")
            return rst

if __name__ == '__main__':

    # 替换为你的文件路径
    file_path = r'C:\Users\env\Downloads\90f31bdc218b225c0a0026b612c0a300-eng.pdf'
    file_path = r'C:\Users\env\Documents\WeChat Files\xxh_430923\FileStorage\File\2023-12\new_summary_2023-12-27_ch.docx'
    file_path = r'C:\Users\env\Downloads\bernste_his_1231.json'
    file_path = r'C:\Users\env\Downloads\Intellistock Service Agreement -20231228_translate.md'
    # file_path = r'C:\Users\env\Downloads\Pinterest, Inc. (PINS) Q3 2023 Earnings Call Transcript _ Seeking Alpha.pdf'
    content = fileUtils().parse_file(file_path)
    print(content.toDict())

