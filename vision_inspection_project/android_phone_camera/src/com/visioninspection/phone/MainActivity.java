package com.visioninspection.phone;

import android.Manifest;
import android.app.Activity;
import android.content.pm.PackageManager;
import android.graphics.ImageFormat;
import android.graphics.Rect;
import android.graphics.RectF;
import android.graphics.Matrix;
import android.graphics.SurfaceTexture;
import android.graphics.YuvImage;
import android.hardware.camera2.*;
import android.media.Image;
import android.media.ImageReader;
import android.net.Uri;
import android.os.*;
import android.util.Range;
import android.util.Size;
import android.view.*;
import android.widget.*;
import org.json.JSONObject;
import java.io.*;
import java.net.*;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.*;

public class MainActivity extends Activity {
    private CameraDevice camera;
    private CameraCaptureSession session;
    private CaptureRequest.Builder request;
    private CameraCharacteristics characteristics;
    private ImageReader reader;
    private HandlerThread cameraThread;
    private Handler handler;
    private TextureView texture;
    private TextView text;
    private ServerSocket server;
    private final ExecutorService clients = Executors.newFixedThreadPool(8);
    private final BlockingQueue<Frame> frames = new ArrayBlockingQueue<>(120);
    private volatile Frame latest;
    private volatile boolean recording, ready;
    private volatile boolean active, opening;
    private volatile String error = "", token = "";
    private volatile double fps = 23, zoom = 1, focus = 0;
    private volatile boolean autoFocus = true;
    private float maxFocus;
    private Range<Float> zoomRange;
    private int width = 1920, height = 1080;
    private long sequence, startTime, nextTime;
    private static class Frame {
        final byte[] jpeg;
        final long sequence;
        final double seconds;
        Frame(byte[] jpeg, long sequence, double seconds) {
            this.jpeg = jpeg; this.sequence = sequence; this.seconds = seconds;
        }
    }
    @Override public void onCreate(Bundle state) {
        super.onCreate(state);
        getWindow().addFlags(WindowManager.LayoutParams.FLAG_KEEP_SCREEN_ON);
        LinearLayout layout = new LinearLayout(this);
        layout.setOrientation(LinearLayout.VERTICAL);
        text = new TextView(this); text.setTextSize(18); text.setPadding(16, 8, 16, 8);
        text.setText("Vision USB Camera - open PC control camera after enabling USB debugging.");
        layout.addView(text);
        texture = new TextureView(this);
        layout.addView(texture, new LinearLayout.LayoutParams(-1, 0, 1));
        setContentView(layout);
        cameraThread = new HandlerThread("VisionCamera"); cameraThread.start();
        handler = new Handler(cameraThread.getLooper());
        texture.setSurfaceTextureListener(new TextureView.SurfaceTextureListener() {
            public void onSurfaceTextureAvailable(SurfaceTexture t, int w, int h) { openCamera(); }
            public void onSurfaceTextureSizeChanged(SurfaceTexture t, int w, int h) { transformPreview(); }
            public boolean onSurfaceTextureDestroyed(SurfaceTexture t) { closeCamera(); return true; }
            public void onSurfaceTextureUpdated(SurfaceTexture t) {}
        });
        new Thread(this::serve, "USBLoopbackServer").start();
        if (checkSelfPermission(Manifest.permission.CAMERA) != PackageManager.PERMISSION_GRANTED)
            requestPermissions(new String[]{Manifest.permission.CAMERA}, 10);
    }
    @Override public void onRequestPermissionsResult(int code, String[] permissions, int[] results) {
        super.onRequestPermissionsResult(code, permissions, results);
        if (code == 10 && results.length > 0 && results[0] == PackageManager.PERMISSION_GRANTED && texture.isAvailable()) openCamera();
    }
    private void show(String message) { runOnUiThread(() -> text.setText(message)); }
    private void openCamera() {
        if (!active || opening || camera != null || checkSelfPermission(Manifest.permission.CAMERA) != PackageManager.PERMISSION_GRANTED) return;
        opening = true;
        try {
            CameraManager manager = (CameraManager)getSystemService(CAMERA_SERVICE);
            String id = null;
            for (String candidate : manager.getCameraIdList()) {
                CameraCharacteristics c = manager.getCameraCharacteristics(candidate);
                if (Objects.equals(c.get(CameraCharacteristics.LENS_FACING), CameraCharacteristics.LENS_FACING_BACK)) { id = candidate; characteristics = c; break; }
            }
            if (id == null) throw new IOException("No rear camera");
            Float distance = characteristics.get(CameraCharacteristics.LENS_INFO_MINIMUM_FOCUS_DISTANCE);
            maxFocus = distance == null ? 0 : distance;
            zoomRange = characteristics.get(CameraCharacteristics.CONTROL_ZOOM_RATIO_RANGE);
            if (zoomRange == null) zoomRange = new Range<>(1f, 1f);
            Size[] sizes = characteristics.get(CameraCharacteristics.SCALER_STREAM_CONFIGURATION_MAP).getOutputSizes(ImageFormat.YUV_420_888);
            Size chosen = null;
            for (Size size : sizes) if (size.getWidth() <= 1920 && size.getHeight() <= 1080 && size.getWidth() % 2 == 0 && size.getHeight() % 2 == 0)
                if (chosen == null || size.getWidth()*size.getHeight() > chosen.getWidth()*chosen.getHeight()) chosen = size;
            if (chosen == null) throw new IOException("No supported video size <= 1920x1080");
            width = chosen.getWidth(); height = chosen.getHeight();
            runOnUiThread(this::transformPreview);
            manager.openCamera(id, new CameraDevice.StateCallback() {
                public void onOpened(CameraDevice device) { opening = false; if (!active) { device.close(); return; } camera = device; configureSession(); }
                public void onDisconnected(CameraDevice device) { opening = false; device.close(); error = "Camera disconnected"; closeCamera(); }
                public void onError(CameraDevice device, int code) { opening = false; device.close(); error = "Camera error " + code; closeCamera(); }
            }, handler);
        } catch (Exception e) { opening = false; error = e.toString(); show(error); }
    }
    private void configureSession() {
        try {
            SurfaceTexture surfaceTexture = texture.getSurfaceTexture();
            surfaceTexture.setDefaultBufferSize(width, height);
            Surface preview = new Surface(surfaceTexture);
            reader = ImageReader.newInstance(width, height, ImageFormat.YUV_420_888, 3);
            reader.setOnImageAvailableListener(this::imageAvailable, handler);
            request = camera.createCaptureRequest(CameraDevice.TEMPLATE_RECORD);
            request.addTarget(preview); request.addTarget(reader.getSurface());
            Range<Integer>[] ranges = characteristics.get(CameraCharacteristics.CONTROL_AE_AVAILABLE_TARGET_FPS_RANGES);
            Range<Integer> rate = null;
            for (Range<Integer> candidate : ranges) if (candidate.getUpper() == 30 && (rate == null || candidate.getLower() > rate.getLower())) rate = candidate;
            if (rate != null) request.set(CaptureRequest.CONTROL_AE_TARGET_FPS_RANGE, rate);
            camera.createCaptureSession(Arrays.asList(preview, reader.getSurface()), new CameraCaptureSession.StateCallback() {
                public void onConfigured(CameraCaptureSession s) { session = s; applyControls(); ready = true; show("USB camera ready: " + width + "x" + height); }
                public void onConfigureFailed(CameraCaptureSession s) { error = "Camera session configuration failed"; }
            }, handler);
        } catch (Exception e) { error = e.toString(); }
    }
    private void transformPreview() {
        if (texture == null || !texture.isAvailable()) return;
        int vw = texture.getWidth(), vh = texture.getHeight();
        int rotation = getWindowManager().getDefaultDisplay().getRotation();
        Matrix matrix = new Matrix();
        RectF view = new RectF(0, 0, vw, vh);
        float cx = view.centerX(), cy = view.centerY();
        if (rotation == Surface.ROTATION_90 || rotation == Surface.ROTATION_270) {
            RectF buffer = new RectF(0, 0, height, width);
            buffer.offset(cx - buffer.centerX(), cy - buffer.centerY());
            matrix.setRectToRect(view, buffer, Matrix.ScaleToFit.FILL);
            float scale = Math.min((float)vw / width, (float)vh / height);
            matrix.postScale(scale, scale, cx, cy);
            matrix.postRotate(90 * (rotation - 2), cx, cy);
        } else {
            float scale = Math.min((float)vw / height, (float)vh / width);
            matrix.setScale(height * scale / vw, width * scale / vh, cx, cy);
            if (rotation == Surface.ROTATION_180) matrix.postRotate(180, cx, cy);
        }
        texture.setTransform(matrix);
    }
    private void applyControls() {
        if (session == null) return;
        try {
            error = "";
            request.set(CaptureRequest.CONTROL_MODE, CaptureRequest.CONTROL_MODE_AUTO);
            request.set(CaptureRequest.CONTROL_ZOOM_RATIO, (float)zoom);
            request.set(CaptureRequest.CONTROL_AF_MODE, autoFocus ? CaptureRequest.CONTROL_AF_MODE_CONTINUOUS_VIDEO : CaptureRequest.CONTROL_AF_MODE_OFF);
            if (!autoFocus) request.set(CaptureRequest.LENS_FOCUS_DISTANCE, (float)focus);
            session.setRepeatingRequest(request.build(), null, handler);
        } catch (Exception e) { error = e.toString(); }
    }
    private void imageAvailable(ImageReader source) {
        Image image = source.acquireLatestImage();
        if (image == null) return;
        try {
            long now = image.getTimestamp();
            synchronized (this) {
                if (recording && startTime == 0) { startTime = now; nextTime = now; }
                if (recording && now < nextTime) return;
                // A late camera callback is not proof that a transmitted frame was lost.
                // Rebase overdue sampling without inventing duplicate camera images.
                if (recording && now - nextTime > (long)(1e9/fps)) nextTime = now;
                if (!recording && latest != null && now - nextTime < 100000000L) return;
                byte[] nv21 = new byte[width * height * 3 / 2];
                Image.Plane[] planes = image.getPlanes();
                for (int p = 0; p < 3; p++) {
                    ByteBuffer buffer = planes[p].getBuffer();
                    int stride = planes[p].getRowStride(), step = planes[p].getPixelStride();
                    int rows = p == 0 ? height : height/2, cols = p == 0 ? width : width/2;
                    if (p == 0 && step == 1) {
                        for (int y = 0; y < rows; y++) {
                            buffer.position(y * stride);
                            buffer.get(nv21, y * width, width);
                        }
                        continue;
                    }
                    for (int y = 0; y < rows; y++) for (int x = 0; x < cols; x++) {
                        int target = p == 0 ? y*width+x : width*height + y*width + x*2 + (p == 1 ? 1 : 0);
                        nv21[target] = buffer.get(y*stride+x*step);
                    }
                }
                ByteArrayOutputStream out = new ByteArrayOutputStream();
                new YuvImage(nv21, ImageFormat.NV21, width, height, null).compressToJpeg(new Rect(0, 0, width, height), 95, out);
                Frame frame = new Frame(out.toByteArray(), recording ? ++sequence : 0, recording ? (now-startTime)/1e9 : 0);
                latest = frame;
                if (recording) {
                    if (!frames.offer(frame)) { error = "USB frame queue overflow; recording stopped"; recording = false; }
                    nextTime += (long)(1e9/fps);
                } else nextTime = now;
            }
        } catch (Exception e) { error = e.toString(); recording = false; }
        finally { image.close(); }
    }
    private JSONObject status() throws Exception {
        return new JSONObject().put("ready", ready).put("recording", recording).put("error", error)
            .put("width", width).put("height", height).put("fps_max", 30).put("zoom", zoom)
            .put("zoom_min", zoomRange == null ? 1 : zoomRange.getLower()).put("zoom_max", zoomRange == null ? 1 : zoomRange.getUpper())
            .put("focus_max", maxFocus).put("focus", focus).put("auto_focus", autoFocus);
    }
    private void serve() {
        try {
            server = new ServerSocket(); server.bind(new InetSocketAddress("127.0.0.1", 8765));
            while (!server.isClosed()) { Socket socket = server.accept(); clients.execute(() -> handle(socket)); }
        } catch (Exception e) { if (server == null || !server.isClosed()) error = e.toString(); }
    }
    private void handle(Socket socket) {
        try (Socket connection = socket) {
            try { handleRequest(connection); }
            catch (Exception e) {
                respond(connection, 400, "application/json", new JSONObject().put("error", e.toString()).toString().getBytes("UTF-8"), "");
            }
        } catch (Exception ignored) {}
    }
    private void handleRequest(Socket connection) throws Exception {
            connection.setSoTimeout(8000);
            BufferedReader input = new BufferedReader(new InputStreamReader(connection.getInputStream(), "UTF-8"));
            String line = input.readLine(); if (line == null) return;
            Uri uri = Uri.parse(line.split(" ")[1]);
            while ((line = input.readLine()) != null && !line.isEmpty()) {}
            String path = uri.getPath(), supplied = uri.getQueryParameter("token");
            if ("/pair".equals(path)) {
                synchronized (this) { if (recording) throw new IOException("Stop recording before pairing"); token = uri.getQueryParameter("new_token"); }
            } else if (token.isEmpty() || !token.equals(supplied)) {
                respond(connection, 403, "application/json", "{\"error\":\"Not paired\"}".getBytes("UTF-8"), ""); return;
            }
            if ("/configure".equals(path)) {
                double z = Double.parseDouble(uri.getQueryParameter("zoom")), f = Double.parseDouble(uri.getQueryParameter("focus"));
                boolean af = Boolean.parseBoolean(uri.getQueryParameter("auto_focus"));
                if (!Double.isFinite(z) || !Double.isFinite(f) || z < zoomRange.getLower() - .0001 || z > zoomRange.getUpper() + .0001 || f < -.0001 || f > maxFocus + .0001 || (!af && maxFocus <= 0)) throw new IOException("Unsupported zoom/focus: zoom=" + z + ", focus=" + f + ", auto_focus=" + af);
                z = Math.max(zoomRange.getLower(), Math.min(zoomRange.getUpper(), z));
                f = Math.max(0, Math.min(maxFocus, f));
                zoom = z; focus = f; autoFocus = af;
                CountDownLatch done = new CountDownLatch(1);
                handler.post(() -> { applyControls(); done.countDown(); });
                if (!done.await(2, TimeUnit.SECONDS)) throw new IOException("Camera control timeout");
            } else if ("/start".equals(path)) {
                synchronized (this) {
                    if (!ready || recording) throw new IOException("Camera unavailable or already recording");
                    fps = Double.parseDouble(uri.getQueryParameter("fps"));
                    if (!Double.isFinite(fps) || fps <= 0 || fps > 30) throw new IOException("FPS must be > 0 and <= 30");
                    error = ""; sequence = 0; startTime = 0; frames.clear(); recording = true;
                }
                show("Recording over USB; controlled by PC");
            } else if ("/stop".equals(path)) {
                synchronized (this) { recording = false; }
                show("Recording stopped; USB camera ready");
            } else if ("/frame".equals(path) || "/preview".equals(path)) {
                Frame frame = "/frame".equals(path) ? frames.poll(2, TimeUnit.SECONDS) : latest;
                if (frame == null) { respond(connection, 204, "image/jpeg", new byte[0], ""); return; }
                respond(connection, 200, "image/jpeg", frame.jpeg,
                    "X-Frame-Number: " + frame.sequence + "\r\nX-Frame-Seconds: " + frame.seconds + "\r\n"); return;
            }
            respond(connection, 200, "application/json", status().toString().getBytes("UTF-8"), "");
    }
    private void respond(Socket socket, int code, String type, byte[] body, String extra) throws IOException {
        OutputStream out = socket.getOutputStream();
        String reason = code == 400 ? "Bad Request" : code == 403 ? "Forbidden" : code == 204 ? "No Content" : "OK";
        out.write(("HTTP/1.1 " + code + " " + reason + "\r\nContent-Type: " + type + "\r\nContent-Length: " + body.length + "\r\nConnection: close\r\n" + extra + "\r\n").getBytes("UTF-8"));
        out.write(body); out.flush();
    }
    private void closeCamera() {
        ready = false; recording = false;
        if (session != null) { session.close(); session = null; }
        if (camera != null) { camera.close(); camera = null; }
        if (reader != null) { reader.close(); reader = null; }
    }
    @Override protected void onPause() { active = false; closeCamera(); super.onPause(); }
    @Override protected void onResume() { super.onResume(); active = true; if (texture != null && texture.isAvailable()) openCamera(); }
    @Override protected void onDestroy() {
        closeCamera(); try { if (server != null) server.close(); } catch (IOException ignored) {}
        clients.shutdownNow(); cameraThread.quitSafely(); super.onDestroy();
    }
}
