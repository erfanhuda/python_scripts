from PIL import Image
import cv2
import pytesseract
import matplotlib.pyplot as plt

file = "D:/dev/GitHub/python_scripts/file/img_20190511_084303.jpg"
temp_file = "D:/dev/GitHub/python_scripts/file/temp_img.jpg"
im = cv2.imread(file)


def display(im_path):
    dpi = 80
    im_data = plt.imread(im_path)
    height, width, depth = im_data.shape

    figsize = width / float(dpi), height / float(dpi)

    fig = plt.figure(figsize=figsize)
    ax = fig.add_axes([0, 0, 1, 1])
    ax.axis('off')

    ax.imshow(im_data, cmap="gray")

    plt.show()


def grayscale(im_path):
    return cv2.cvtColor(im_path, cv2.COLOR_BGR2GRAY)


gray_img = grayscale(file)
cv2.imwrite(temp_file, gray_img)
display(temp_file)
