# Method 1-

import os
import img2pdf
with open("out.pdf", "wb") as file:
   file.write(img2pdf.convert([i for i in os.listdir('Path of image_Directory') if i.endswith(".jpg")]))

# Method 2 -

from fpdf import FPDF
Pdf = FPDF()
list_of_images = ["one.jpg", "second.jpg","third.jpg"]
for i in list_of_images:
   Pdf.add_page()
   Pdf.image(i,x,y,w,h)
   Pdf.output("out.pdf", "F")