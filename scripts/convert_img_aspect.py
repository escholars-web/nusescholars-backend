#!/usr/bin/env python3
"""
Gradio app: detect the largest face, crop to a 1200x1500 portrait, and allow post-processing adjustments.
"""

import os
import tempfile
from pathlib import Path
from typing import Optional, Tuple

import cv2
import gradio as gr
import numpy as np

TARGET_W = 1200
TARGET_H = 1500
TARGET_RATIO = TARGET_W / TARGET_H  # 0.8


def clamp(value, min_value, max_value):
    return max(min_value, min(value, max_value))


def detect_largest_face(image_bgr: np.ndarray) -> Optional[Tuple[int, int, int, int]]:
    gray = cv2.cvtColor(image_bgr, cv2.COLOR_BGR2GRAY)
    cascade_path = cv2.data.haarcascades + "haarcascade_frontalface_default.xml"
    face_cascade = cv2.CascadeClassifier(cascade_path)

    faces = face_cascade.detectMultiScale(
        gray,
        scaleFactor=1.1,
        minNeighbors=5,
        flags=cv2.CASCADE_SCALE_IMAGE,
        minSize=(60, 60),
    )

    if len(faces) == 0:
        return None

    return max(faces, key=lambda rect: rect[2] * rect[3])


def compute_base_crop(face_rect, img_w, img_h):
    if face_rect is None:
        center_x = img_w / 2
        center_y = img_h / 2
        crop_h = img_h
    else:
        x, y, w, h = face_rect
        center_x = x + w / 2
        center_y = y + h / 2
        desired_face_coverage = 0.4  # fraction of crop height
        crop_h = h / desired_face_coverage

    crop_h = clamp(crop_h, 200, img_h)
    crop_w = crop_h * TARGET_RATIO

    if crop_w > img_w:
        crop_w = img_w
        crop_h = crop_w / TARGET_RATIO

    x1 = center_x - crop_w / 2
    y1 = center_y - crop_h / 2
    x1 = clamp(x1, 0, img_w - crop_w)
    y1 = clamp(y1, 0, img_h - crop_h)

    return int(round(x1)), int(round(y1)), int(round(crop_w)), int(round(crop_h))


def adjust_crop(base_rect, offset_x_pct, offset_y_pct, scale_factor, img_w, img_h):
    bx, by, bh = base_rect[0], base_rect[1], base_rect[3]
    base_height = bh
    new_height = clamp(base_height * scale_factor, 120, img_h)
    new_width = new_height * TARGET_RATIO

    if new_width > img_w:
        new_width = img_w
        new_height = new_width / TARGET_RATIO
    if new_height > img_h:
        new_height = img_h
        new_width = new_height * TARGET_RATIO

    center_x = bx + base_rect[2] / 2
    center_y = by + base_rect[3] / 2

    half_w = new_width / 2
    half_h = new_height / 2

    center_x = clamp(center_x, half_w, img_w - half_w)
    center_y = clamp(center_y, half_h, img_h - half_h)

    max_left = center_x - half_w
    max_right = img_w - (center_x + half_w)
    max_up = center_y - half_h
    max_down = img_h - (center_y + half_h)

    if offset_x_pct >= 0:
        shift_x = (offset_x_pct / 100.0) * max_right
    else:
        shift_x = (offset_x_pct / 100.0) * max_left

    if offset_y_pct >= 0:
        shift_y = (offset_y_pct / 100.0) * max_down
    else:
        shift_y = (offset_y_pct / 100.0) * max_up

    center_x = clamp(center_x + shift_x, half_w, img_w - half_w)
    center_y = clamp(center_y + shift_y, half_h, img_h - half_h)

    x1 = int(round(center_x - half_w))
    y1 = int(round(center_y - half_h))
    w = int(round(new_width))
    h = int(round(new_height))

    return x1, y1, w, h


def finalize_crop(image_bgr, crop_rect):
    x, y, w, h = crop_rect
    cropped = image_bgr[y:y + h, x:x + w].copy()
    resized = cv2.resize(cropped, (TARGET_W, TARGET_H), interpolation=cv2.INTER_LANCZOS4)
    result_rgb = cv2.cvtColor(resized, cv2.COLOR_BGR2RGB)

    fd, path = tempfile.mkstemp(suffix=".png")
    os.close(fd)
    cv2.imwrite(path, resized)

    return result_rgb, path


def handle_detect(image):
    if image is None:
        raise gr.Error("Please upload an image first.")

    image_bgr = cv2.cvtColor(image, cv2.COLOR_RGB2BGR)
    img_h, img_w = image_bgr.shape[:2]

    face_rect = detect_largest_face(image_bgr)
    crop_rect = compute_base_crop(face_rect, img_w, img_h)
    result_rgb, path = finalize_crop(image_bgr, crop_rect)

    return (
        result_rgb,
        path,
        image_bgr,
        crop_rect,
        gr.update(value=0),
        gr.update(value=0),
        gr.update(value=1.0),
    )


def handle_adjust(offset_x, offset_y, scale, original_bgr, base_rect):
    if original_bgr is None or base_rect is None:
        raise gr.Error("Upload and process an image before adjusting.")

    img_h, img_w = original_bgr.shape[:2]
    adjusted_rect = adjust_crop(base_rect, offset_x, offset_y, scale, img_w, img_h)
    return finalize_crop(original_bgr, adjusted_rect)


def handle_reset(original_bgr, base_rect):
    if original_bgr is None or base_rect is None:
        return None, None, gr.update(value=0), gr.update(value=0), gr.update(value=1.0)

    result_rgb, path = finalize_crop(original_bgr, base_rect)
    return result_rgb, path, gr.update(value=0), gr.update(value=0), gr.update(value=1.0)


with gr.Blocks(title="Face-Centered 1200×1500 Resizer") as demo:
    gr.Markdown(
        """
# Face-Centered 1200×1500 Resizer
1. Upload an image and click **Detect & Resize**.<br>
2. Use the sliders to shift or zoom the crop if needed.<br>
3. Download the adjusted portrait.
"""
    )

    with gr.Row():
        input_image = gr.Image(type="numpy", label="Upload Image")
        output_image = gr.Image(type="numpy", label="Processed Preview")

    download_file = gr.File(label="Download Processed Image")

    original_state = gr.State()
    base_crop_state = gr.State()

    with gr.Accordion("Post-processing adjustments", open=True):
        offset_x = gr.Slider(-100, 100, value=0, step=1, label="Horizontal shift (%)")
        offset_y = gr.Slider(-100, 100, value=0, step=1, label="Vertical shift (%)")
        scale_slider = gr.Slider(
            0.7, 10.0, value=1.0, step=0.2, label="Crop scale (relative to auto)"
        )
        with gr.Row():
            process_btn = gr.Button("Detect & Resize", variant="primary")
            apply_btn = gr.Button("Apply adjustments")
            reset_btn = gr.Button("Reset adjustments")

    process_btn.click(
        handle_detect,
        inputs=input_image,
        outputs=[
            output_image,
            download_file,
            original_state,
            base_crop_state,
            offset_x,
            offset_y,
            scale_slider,
        ],
    )

    apply_btn.click(
        handle_adjust,
        inputs=[offset_x, offset_y, scale_slider, original_state, base_crop_state],
        outputs=[output_image, download_file],
    )

    reset_btn.click(
        handle_reset,
        inputs=[original_state, base_crop_state],
        outputs=[output_image, download_file, offset_x, offset_y, scale_slider],
    )

if __name__ == "__main__":
    demo.launch()