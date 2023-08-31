import aspose.pdf as ap
import fitz
import os

# Имя pdf - файла для замены
pdf_in = "pdf_in.pdf"
# Временный файл с заменой, но watermark
pdf_temp = "pdf_temp.pdf"
# Итоговый файл
pdf_out= "pdf_out.pdf"

# Что меняем
repl_what = "июля"
# На что меняем
repl_to = "июня"

# Загружаем PDF-документ
document = ap.Document(pdf_in)

# Создание экземпляра объекта TextFragmentAbsorber
txtAbsorber = ap.text.TextFragmentAbsorber(repl_what)

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
    # Ристуем белый прямоугольник на watermark
    page.draw_rect([1,1,300,20],   color=None, fill=fitz.utils.getColor('white'), overlay=True, fill_opacity=1)
# Save pdf
doc.save(pdf_out)
doc.close()
# удаляем временный
os.remove(pdf_temp)
