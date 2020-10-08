from PIL import Image
import requests
from io import BytesIO
import numpy as np
import matplotlib.pyplot as plt

def get_img_obj(Field, ID, filename=None):
    url = f'http://143.107.18.89:8000/media/{Field}/{ID}.jpg'
    response = requests.get(url)
    img = Image.open(BytesIO(response.content))

    if filename:
        img2 = np.array(img)
        plt.imsave(arr=img2, fname=filename)

    return img
