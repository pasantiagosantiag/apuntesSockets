package ies.sequeros.psp.socketniotcp.server;


import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

public class ClientInServer implements AutoCloseable, Runnable {

    private final BlockingQueue<byte[]> colaSalida = new LinkedBlockingQueue<>();
    private AtomicBoolean serverRunning;
    private AtomicBoolean internalRunning;
    private SocketChannel socket;

    private Consumer<ClientInServer> onClose;
    private BiConsumer<ClientInServer, String> onMessageReceived;
    private Thread readThread;
    private Thread writeThread;
    private String clienteIP;
    private int clientePort;

    public ClientInServer(
            SocketChannel socket,
            AtomicBoolean serverRunning,
            Consumer<ClientInServer> onClose,
            BiConsumer<ClientInServer, String> onMessageReceived
    ) {

        this.socket = socket;
        this.serverRunning = serverRunning;
        this.onClose = onClose;
        this.internalRunning = new AtomicBoolean(true);
        this.onMessageReceived = onMessageReceived;
        this.clienteIP = socket.socket().getInetAddress().getHostAddress();
        this.clientePort = socket.socket().getPort();
    }

    @Override
    public String toString() {
        return this.hashCode() + " IP:" + this.clienteIP + " Port:" + this.clientePort;
    }

    @Override
    public void close() {
        // compareAndSet asegura que el cierre y el evento onClose se ejecuten UNA sola vez
        if (internalRunning.compareAndSet(true, false)) {
            try {
                if (socket != null && socket.isConnected()) {
                    socket.close(); // Desbloquea inmediatamente entrada.readLine()
                }
            } catch (IOException e) {
                System.err.println("Error cerrando socket de cliente: " + e.getMessage());
            } finally {
                if (onClose != null) {
                    onClose.accept(this);
                }
            }
        }
    }

    @Override
    public void run() {

        this.readThread = Thread.startVirtualThread(() -> {

            try (BufferedInputStream entrada = new BufferedInputStream(Channels.newInputStream(this.socket))) {
                byte[] buffer = new byte[1024];
                int leidos;
                while ((leidos = entrada.read(buffer)) != -1) {
                    String mensaje = new String(buffer, 0, leidos, StandardCharsets.UTF_8).trim();

                    // Se avisa al servidor de que ha llegado un mensaje
                    this.onMessageReceived.accept(this, mensaje);
                }

            } catch (IOException e) {
                throw new RuntimeException(e);
            } finally {
                //se avisa que se ha terminado
                if (this.onClose != null)
                    this.onClose.accept(this);
                close();
            }
        });
        //this.readThread.start();
        this.writeThread = Thread.startVirtualThread(() -> {
            try (BufferedOutputStream salida = new BufferedOutputStream(Channels.newOutputStream(socket))) {
                while (serverRunning.get() && internalRunning.get() && socket.isOpen()) {
                    byte[] datos = colaSalida.take(); // Espera sin consumir CPU hasta que haya datos
                    salida.write(datos);
                    salida.flush();
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }finally {
                close();
            }
        });
        try {
            //esperar a que terminen los hijos
            this.readThread.join();
            this.writeThread.join();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
    public void writeMessage(String texto) {
        if (serverRunning.get() && internalRunning.get() && socket.isOpen()) {
            colaSalida.offer(texto.getBytes(StandardCharsets.UTF_8));
        }
    }
}