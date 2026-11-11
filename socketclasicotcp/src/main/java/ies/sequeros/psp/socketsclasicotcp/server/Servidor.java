package ies.sequeros.psp.socketsclasicotcp.server;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
public class Servidor {
    private final int port;
    private ServerSocket socketServidorTCP;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final List<ClientInServer> clients = new CopyOnWriteArrayList<>();
    private TerminalThread terminalThread;
    private Thread socketThread;

    public Servidor(int port) {
        this.port = port;
    }

    public void start() {
        running.set(true);
        // Hilo encargado de leer el teclado y terminar el servidor
        this.terminalThread = new TerminalThread(running, (msg) -> {
            System.out.println("Terminal: " + msg.toString());
            try {
                close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
        this.terminalThread.start();
        try {
            this.socketServidorTCP = new ServerSocket(this.port);
            // Hilo para aceptar conexiones de nuevos clientes
            this.socketThread = new Thread(() -> {
                while (running.get()) {
                    System.out.println("Esperando conexiones en el puerto: " + port);
                    try {
                        Socket socket = socketServidorTCP.accept();
                        System.out.println("Nuevo cliente conectado.");

                        ClientInServer clientInServer = new ClientInServer(
                                socket, running, this::onClientDisconnect, this::onMessageReceived
                        );
                        clientInServer.start();
                        this.retransmissionMessage(clientInServer,"Nuevo cliente conectado:"+clientInServer.toString());
                        this.clients.add(clientInServer);
                    } catch (Exception e) {
                        if (!running.get()) {
                            // Cierre ordenado provocado
                            break;
                        } else {
                            System.err.println("Error inesperado en el accept: " + e.getMessage());
                        }
                    }
                }
            });
            this.socketThread.start();

        } catch (IOException e) {
            throw new RuntimeException("No se pudo iniciar el servidor en el puerto " + port, e);
        }
    }
    private void onClientDisconnect(ClientInServer clientInServer) {
        try {
            clientInServer.close();
        } catch (Exception e) {
            System.err.println("Error al cerrar cliente desconectado: " + e.getMessage());
        }
        clients.remove(clientInServer);
    }
    private void onMessageReceived(ClientInServer cliente, String msg) {
        Command c = Command.fromDescription(msg);
        if (c != null) {
            this.processCommand(c, cliente);
        } else {
            this.retransmissionMessage(cliente, "Mensaje (" + cliente.toString() + "): " + msg);
            System.out.println("Mensaje de " + cliente.hashCode() + ": " + msg);
        }
    }
    /**
     * Retransmite los mensajes a todos los clientes excepto al remitente.
     */
    private void retransmissionMessage(ClientInServer clientInServer, String message) {
        for (ClientInServer client : clients) {
            if (client != clientInServer) {
                client.writeMessage(message);
            }
        }
    }
    /**
     * Procesa los comandos enviados por los clientes.
     */
    private void processCommand(Command c, ClientInServer client) {
        switch (c) {
            case QUIT:
                System.out.println("Cliente desconectado por comando: " + c);
                this.retransmissionMessage(client, "Cliente desconectado (" + c + ")");
                client.close();
                break;

            case LISTCLIENTS:
                StringBuilder sb = new StringBuilder("Listado de clientes:\n");
                for (ClientInServer cliente : clients) {
                    if (cliente != null) {
                        sb.append("CLIENTE: ").append(cliente.toString()).append("\n");
                    }
                }
                client.writeMessage(sb.toString());
                break; // Añadido el break faltante por buenas prácticas

            default:
                break;
        }
    }

    public void close() throws IOException {
        running.set(false);

        // Cerrar el ServerSocket para desbloquear inmediatamente el accept()
        if (this.socketServidorTCP != null && !this.socketServidorTCP.isClosed()) {
            this.socketServidorTCP.close();
        }

        // Cerrar a todos los clientes de forma segura e independiente
        for (ClientInServer client : clients) {
            try {
                client.close();
            } catch (Exception e) {
                System.err.println("Error al cerrar un cliente durante el apagado: " + e.getMessage());
            }
        }
        clients.clear();

        // Interrumpir hilo de terminal (Nota: System.in seguirá bloqueado hasta recibir un ENTER)
        if (this.terminalThread != null) {
            this.terminalThread.interrupt();
        }

        System.out.println("Servidor cerrado correctamente.");
    }
    public static void main(String[] args) {
        Servidor servidor = new Servidor(5000);
        servidor.start();
    }
}