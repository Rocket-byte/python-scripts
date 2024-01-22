import cv2 as cv

# Author: Ruslana Kruk

def detect_faces_and_eyes():
    """
    Detects faces and eyes using the webcam in real-time.
    Uses Haar cascades for detection.
    Press 'q' to exit.
    """
    # Load Haar cascades for face and eye detection
    face_cascade = cv.CascadeClassifier("haarcascade_frontalface_default.xml")
    eye_cascade = cv.CascadeClassifier("haarcascade_eye.xml")

    # Initialize webcam
    cap = cv.VideoCapture(0)

    while cap.isOpened():
        ret, img = cap.read()
        if not ret:
            break

        gray = cv.cvtColor(img, cv.COLOR_BGR2GRAY)
        faces = face_cascade.detectMultiScale(gray, 1.1, 7)
        eyes = eye_cascade.detectMultiScale(gray, 1.1, 7)

        # Draw rectangles around detected faces and eyes
        for (x, y, w, h) in faces:
            cv.rectangle(img, (x, y), (x + w, y + h), (0, 255, 0), 1)
        for (x, y, w, h) in eyes:
            cv.rectangle(img, (x, y), (x + w, y + h), (255, 0, 0), 1)

        cv.imshow("Detection", img)

        # Exit on 'q' key
        if cv.waitKey(1) & 0xFF == ord('q'):
            break

    cap.release()
    cv.destroyAllWindows()

def contour_analysis():
    """
    Performs contour analysis on a hand image.
    Displays thresholded, contours, and convex hull images.
    """
    img = cv.imread("hand1.jpg", cv.IMREAD_GRAYSCALE)
    _, thresh = cv.threshold(img, 70, 255, cv.THRESH_BINARY)
    contours, _ = cv.findContours(thresh.copy(), cv.RETR_TREE, cv.CHAIN_APPROX_SIMPLE)

    convex_hulls = [cv.convexHull(c) for c in contours]

    cv.imshow("Original", img)
    cv.imshow("Thresholded", thresh)
    cv.imshow("Contours", cv.drawContours(img.copy(), contours, -1, (0, 0, 0), 2))
    cv.imshow("Convex Hulls", cv.drawContours(img.copy(), convex_hulls, -1, (0, 0, 0), 2))

    cv.waitKey(0)
    cv.destroyAllWindows()

if __name__ == "__main__":
    detect_faces_and_eyes()
    contour_analysis()
