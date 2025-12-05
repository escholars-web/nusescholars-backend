import os
import requests
from PIL import Image
import cv2
from app.database.supabase_client import supabase
import re
from http.cookiejar import MozillaCookieJar

# === DIRECTORIES ===
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DOWNLOAD_DIR = os.path.join(BASE_DIR, "cropped_img")
CROPPED_DIR = os.path.join(BASE_DIR, "cropped")
os.makedirs(DOWNLOAD_DIR, exist_ok=True)
os.makedirs(CROPPED_DIR, exist_ok=True)

# === FACE CROP ===
CASCADE_PATH = cv2.data.haarcascades + "haarcascade_frontalface_default.xml"
face_cascade = cv2.CascadeClassifier(CASCADE_PATH)
CROP_SCALE = 3.0

def crop_face_centered(image_path, output_path):
    image = cv2.imread(image_path)
    if image is None:
        print(f"Cannot read image: {image_path}")
        return False
    gray = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)
    faces = face_cascade.detectMultiScale(gray, scaleFactor=1.1, minNeighbors=5)

    if len(faces) == 0:
        print(f"No face found in {image_path}")
        return False

    x, y, w, h = max(faces, key=lambda f: f[2] * f[3])
    center_x = x + w // 2
    center_y = y + h // 2
    face_size = max(w, h)
    crop_height = int(face_size * CROP_SCALE)
    crop_width = int(crop_height * 3 / 4)

    img_h, img_w = image.shape[:2]
    half_w = crop_width // 2
    half_h = crop_height // 2
    left = max(center_x - half_w, 0)
    top = max(center_y - half_h, 0)
    right = min(center_x + half_w, img_w)
    bottom = min(center_y + half_h, img_h)
    cropped = image[top:bottom, left:right]

    pil_image = Image.fromarray(cv2.cvtColor(cropped, cv2.COLOR_BGR2RGB))
    pil_image.save(output_path)
    print(f"Saved cropped face to {output_path}")
    return True

def sanitize_filename(name):
    name = re.sub(r'[^a-zA-Z0-9_\-\.]', '_', name)
    return name.strip("_")

# === COOKIES ===
COOKIES_FILE = os.path.abspath(os.path.join(BASE_DIR, "../../data/cookies.txt"))

def get_authenticated_session(cookiefile=COOKIES_FILE):
    session = requests.Session()
    cj = MozillaCookieJar(cookiefile)
    cj.load(cookiefile, ignore_discard=True, ignore_expires=True)
    session.cookies.update({c.name: c.value for c in cj})
    return session

def download_image_with_session(session, url, filename):
    try:
        response = session.get(url, timeout=15)
        response.raise_for_status()
        with open(filename, "wb") as f:
            f.write(response.content)
        return True
    except Exception as e:
        print(f"Failed to download {url}: {e}")
        return False

# === SUPABASE: PULL PROFILES ===
def get_staging_profiles():
    res = supabase.table("staging").select("full_name, picture_url").execute()
    rows = res.data if hasattr(res, "data") else res['data']
    return [r for r in rows if r.get("picture_url")]

def get_file_extension(url):
    ext = os.path.splitext(url.split("?")[0])[1]
    if ext.lower() in ['.jpg', '.jpeg', '.png']:
        return ext
    return ".jpg"  # Default to jpg

def main():
    session = get_authenticated_session()
    profiles = get_staging_profiles()
    print(f"Found {len(profiles)} profiles with picture_url.")

    for i, profile in enumerate(profiles):
        url = profile.get("picture_url")
        full_name = profile.get("full_name", f"user_{i}")
        ext = get_file_extension(url)
        base_filename = sanitize_filename(full_name.replace(" ", "_")) + ext
        local_img_path = os.path.join(DOWNLOAD_DIR, base_filename)

        # Download with cookies/session
        if not os.path.exists(local_img_path):
            success = download_image_with_session(session, url, local_img_path)
            if not success:
                continue

        # Crop
        cropped_img_path = os.path.join(CROPPED_DIR, base_filename)
        crop_face_centered(local_img_path, cropped_img_path)

if __name__ == "__main__":
    main()