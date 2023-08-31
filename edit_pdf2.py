import aspose.pdf as ap
import fitz

# Имя pdf - файла для замены
pdf_in = "pdf_in.pdf"
pdf_temp = "pdf_temp.pdf"
pdf_out= "pdf_out.pdf"

repl_from = "июля"
repl_to = "июня"

# Загружаем PDF-документ
document = ap.Document(pdf_in)

# Создание экземпляра объекта TextFragmentAbsorber
txtAbsorber = ap.text.TextFragmentAbsorber(repl_from)

# Поиск текста
document.pages.accept(txtAbsorber)

# Получить ссылку на найденные фрагменты текста
textFragmentCollection = txtAbsorber.text_fragments

# Разобрать все найденные фрагменты текста и заменить текст
for txtFragment in textFragmentCollection:
    txtFragment.text = repl_to

# Сохраните обновленный PDF
document.save(pdf_temp)

doc = fitz.open(pdf_temp)
for page in doc:
    # For every page, draw a rectangle on coordinates (1,1)(100,100)
    page.draw_rect([1,1,300,20],   color=None, fill=fitz.utils.getColor('white'), overlay=True, fill_opacity=1)
# Save pdf
doc.save(pdf_out)