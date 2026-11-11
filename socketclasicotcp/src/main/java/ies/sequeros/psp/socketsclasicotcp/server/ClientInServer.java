package ies.sequeros.psp.socketsclasicotcp.server;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
public class ClientInServer implements AutoCloseable {

    private final AtomicBoolean serverRunning;
    private final AtomicBoolean internalRunning;
    private final Socket socket;
    private final PrintWriter salida;

    private Consumer<ClientInServer> onClose;
    private BiConsumer<ClientInServer, String> onMessageReceived;
    private Thread readThread;
    private final String clienteIP;
    private final int clientePort;

    public ClientInServer(
            Socket socket,
            AtomicBoolean serverRunning,
            Consumer<ClientInServer> onClose,
            BiConsumer<ClientInServer, String> onMessageReceived
    ) throws IOException {

        this.socket = socket;
        this.serverRunning = serverRunning;
        this.onClose = onClose;
        this.onMessageReceived = onMessageReceived;
        this.internalRunning = new AtomicBoolean(true);
        this.salida = new PrintWriter(socket.getOutputStream(), true);
        this.clienteIP = socket.getInetAddress().getHostAddress();
        this.clientePort = socket.getPort();
    }
    @Override
    public String toString() {
        return this.hashCode() + " IP:" + this.clienteIP + " Port:" + this.clientePort;
    }
    public void start() {
        this.readThread = new Thread(() -> {
            try (BufferedReader entrada = new BufferedReader(new InputStreamReader(socket.getInputStream()))) {
                while (serverRunning.get() && internalRunning.get()) {
                    String line = entrada.readLine();

                    // El cliente cerró la conexión abruptamente (fin del stream)
                    if (line == null) {
                        break;
                    }
                    // Se delega el mensaje (sea texto o comando como "quit") al servidor
                    if (onMessageReceived != null) {
                        onMessageReceived.accept(this, line);
                    }
                }
            } catch (IOException e) {
                // Si el servidor o el cliente cierran el socket de forma intencionada, no es un error crítico
                if (serverRunning.get() && internalRunning.get()) {
                    System.err.println("Conexión perdida con cliente " + toString() + ": " + e.getMessage());
                }
            } finally {
                // Garantiza que siempre se limpie y avise a la capa superior al morir el hilo
                close();
            }
        });
        this.readThread.setName("ClientReader-" + clientePort);
        this.readThread.start();
    }
    /**
     * Envía un mensaje al cliente de forma sincronizada para evitar mezcla de bytes.
     */
    public synchronized void writeMessage(final String message) {
        if (internalRunning.get() && !socket.isClosed()) {
            this.salida.println(message);
        }
    }
    public void setOnClose(final Consumer<ClientInServer> onClose) {
        this.onClose = onClose;
    }

    public void setOnMessageReceived(final BiConsumer<ClientInServer, String> onMessageReceived) {
        this.onMessageReceived = onMessageReceived;
    }

    @Override
    public void close() {
        // compareAndSet asegura que el cierre y el evento onClose se ejecuten UNA sola vez
        if (internalRunning.compareAndSet(true, false)) {
            try {
                if (salida != null) {
                    salida.close();
                }
                if (socket != null && !socket.isClosed()) {
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
}