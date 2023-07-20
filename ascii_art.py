from PIL import Image, ImageFont, ImageDraw
import pytesseract

 
def string_to_bitmap(text, font_style='arialbd.ttf', font_weight=9):
	font = ImageFont.truetype(font_style, font_weight)  # load the font
	size = font.getsize(text)  # claculate the size of text in pixels
	image = Image.new('1', size, 'white')  # create a white image
	draw = ImageDraw.Draw(image)
	draw.text((0, 0), text, font=font)  # render the text to bitmap
	return image


def map_bit_to_char(image, column, row, char='#'):
	return ' ' if image.getpixel((column, row)) else char


def bitmap_to_ascii_art(bitmap):
	result = ''
	for row in range(bitmap.height):
		line_list = [map_bit_to_char(bitmap, column, row) for column in range(bitmap.width)]
		result += ''.join(line_list) + '\n'
	return result

def ascii_art_to_image(ascii_art, char='#'):
	lines = ascii_art.splitlines()
	size = (len(lines[0]), len(lines))
	image = Image.new( '1', size, "white") # Create a new white image
	pixels = image.load() # Create the pixel map
	for i in range(image.width):    # For every pixel:
		for j in range(image.height):
			if lines[j][i] == char:
				pixels[i, j] = 0 
				# Make that pixel black	
	return image


def text_to_ascii_art(text, font_style='arialbd.ttf', font_weight=9):
	bitmap = string_to_bitmap(text, font_style, font_weight)
	return bitmap_to_ascii_art(bitmap)


def ascii_art_to_text(ascii_art, char='#'):
	image = ascii_art_to_image(ascii_art, char)
	return pytesseract.image_to_string(image)
