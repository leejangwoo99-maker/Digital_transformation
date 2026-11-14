from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import cv2
import numpy as np



@dataclass
class AlignmentResult:
    image: object = None
    matrix: object = None
    valid_mask: object = None
    message: str = "alignment_failed"

    @property
    def ok(self):
        return self.image is not None


def project_box(box, matrix, shape):
    x1, y1, x2, y2 = box
    corners = np.float32([[x1, y1], [x2, y1], [x2, y2], [x1, y2]]).reshape(-1, 1, 2)
    points = cv2.perspectiveTransform(corners, matrix).reshape(-1, 2)
    height, width = shape[:2]
    return (max(0, int(np.floor(points[:, 0].min()))), max(0, int(np.floor(points[:, 1].min()))),
            min(width, int(np.ceil(points[:, 0].max()))), min(height, int(np.ceil(points[:, 1].max()))))


class FrameAligner:
    def __init__(self, reference_path: Path):
        self.reference_path = Path(reference_path)
        self.reference = cv2.imread(str(reference_path))
        if self.reference is None:
            raise RuntimeError(f"Alignment reference cannot be read: {reference_path}")
        self.sift = cv2.SIFT_create(nfeatures=3000, contrastThreshold=0.025)
        small, self.reference_scale = self.small_image(self.reference)
        self.reference_mask = np.full(self.reference.shape[:2], 255, dtype=np.uint8)
        small_mask = cv2.resize(self.reference_mask, (small.shape[1], small.shape[0]), interpolation=cv2.INTER_NEAREST)
        self.reference_points, self.reference_descriptors = self.sift.detectAndCompute(small, small_mask)
        if self.reference_descriptors is None or len(self.reference_points) < 12:
            raise RuntimeError("Training frame has too few stable features for automatic alignment.")
        self.small_reference_mask = small_mask
        self.matcher = cv2.BFMatcher(cv2.NORM_L2)

    @staticmethod
    def small_image(image):
        scale = min(1.0, 1000 / max(image.shape[:2]))
        small = cv2.resize(image, None, fx=scale, fy=scale, interpolation=cv2.INTER_AREA) if scale < 1 else image
        return cv2.cvtColor(small, cv2.COLOR_BGR2GRAY), scale

    def align(self, frame) -> AlignmentResult:
        gray, scale = self.small_image(frame)
        points, descriptors = self.sift.detectAndCompute(gray, None)
        if descriptors is None or len(points) < 12:
            return AlignmentResult(message="alignment_failed: too few camera features")
        matches = self.matcher.knnMatch(descriptors, self.reference_descriptors, k=2)
        good = [pair[0] for pair in matches if len(pair) == 2 and pair[0].distance < 0.72 * pair[1].distance]
        if len(good) < 12:
            return AlignmentResult(message="alignment_failed: product not matched")
        source = np.float32([points[match.queryIdx].pt for match in good]).reshape(-1, 1, 2)
        target = np.float32([self.reference_points[match.trainIdx].pt for match in good]).reshape(-1, 1, 2)
        matrix, inliers = cv2.findHomography(source, target, cv2.RANSAC, 3.0)
        if matrix is None or inliers is None or not np.isfinite(matrix).all():
            return AlignmentResult(message="alignment_failed: unstable transform")
        good_mask = inliers.ravel().astype(bool)
        count = int(good_mask.sum())
        if count < 12 or count / len(good) < 0.4:
            return AlignmentResult(message="alignment_failed: unreliable feature matches")
        hull = cv2.convexHull(target[good_mask])
        foreground_area = cv2.countNonZero(self.small_reference_mask)
        if cv2.contourArea(hull) < foreground_area * 0.06:
            return AlignmentResult(message="alignment_failed: matches cover too little of product")
        source_scale = np.diag([scale, scale, 1.0])
        reference_scale = np.diag([1 / self.reference_scale, 1 / self.reference_scale, 1.0])
        matrix = reference_scale @ matrix @ source_scale
        if abs(np.linalg.det(matrix)) < 1e-10:
            return AlignmentResult(message="alignment_failed: singular transform")
        height, width = self.reference.shape[:2]
        ys, xs = np.where(self.reference_mask > 0)
        product_corners = np.float32([[xs.min(), ys.min()], [xs.max(), ys.min()],
                                     [xs.max(), ys.max()], [xs.min(), ys.max()]]).reshape(-1, 1, 2)
        projected = cv2.perspectiveTransform(product_corners, np.linalg.inv(matrix))
        area = abs(cv2.contourArea(projected))
        source_area = frame.shape[0] * frame.shape[1]
        if not cv2.isContourConvex(projected) or not 0.02 * source_area < area < 4 * source_area:
            return AlignmentResult(message="alignment_failed: implausible product geometry")
        valid = cv2.warpPerspective(np.full(frame.shape[:2], 255, dtype=np.uint8), matrix, (width, height),
                                    flags=cv2.INTER_NEAREST)
        coverage = np.count_nonzero((valid > 0) & (self.reference_mask > 0)) / np.count_nonzero(self.reference_mask)
        if coverage < 0.94:
            return AlignmentResult(message=f"alignment_failed: product cut off ({coverage:.0%} visible)")
        image = cv2.warpPerspective(frame, matrix, (width, height), flags=cv2.INTER_LINEAR)
        return AlignmentResult(image, matrix, valid, f"aligned: {count} matches / {coverage:.0%} visible")
