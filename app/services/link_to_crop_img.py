import cv2
from PIL import Image
import os

# Base directory = same as script location
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
INPUT_DIR = BASE_DIR
OUTPUT_DIR = os.path.join(BASE_DIR, "cropped")

os.makedirs(OUTPUT_DIR, exist_ok=True)

# Load OpenCV's pre-trained face detector
CASCADE_PATH = cv2.data.haarcascades + "haarcascade_frontalface_default.xml"
face_cascade = cv2.CascadeClassifier(CASCADE_PATH)

# How much extra space around the face (default 3x face height)
CROP_SCALE = 3.0  

def crop_face_centered(image_path, output_path):
    # Load image in OpenCV
    image = cv2.imread(image_path)
    gray = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)

    # Detect faces
    faces = face_cascade.detectMultiScale(gray, scaleFactor=1.1, minNeighbors=5)

    if len(faces) == 0:
        print(f"No face found in {image_path}")
        return

    # Pick the largest face
    x, y, w, h = max(faces, key=lambda f: f[2] * f[3])

    # Face center
    center_x = x + w // 2
    center_y = y + h // 2

    # Crop size (with extra space)
    face_size = max(w, h)
    crop_height = int(face_size * CROP_SCALE)
    crop_width = int(crop_height * 3 / 4)  # 4:3 aspect ratio (portrait)

    # Ensure crop fits within image boundaries
    img_h, img_w = image.shape[:2]
    half_w = crop_width // 2
    half_h = crop_height // 2

    left = max(center_x - half_w, 0)
    top = max(center_y - half_h, 0)
    right = min(center_x + half_w, img_w)
    bottom = min(center_y + half_h, img_h)

    cropped = image[top:bottom, left:right]

    # Save as-is (keep natural resolution)
    pil_image = Image.fromarray(cv2.cvtColor(cropped, cv2.COLOR_BGR2RGB))
    pil_image.save(output_path)
    print(f"Saved cropped face to {output_path}")

# Process all images in script folder
for filename in os.listdir(INPUT_DIR):
    if filename.lower().endswith((".jpg", ".jpeg", ".png")):
        input_path = os.path.join(INPUT_DIR, filename)
        output_path = os.path.join(OUTPUT_DIR, filename)
        crop_face_centered(input_path, output_path)
