package ies.sequeros.psp.socketniotcp.server;

import ies.sequeros.psp.socketniotcp.TerminalThread;
import ies.sequeros.psp.socketsclasicotcp.server.Command;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.atomic.AtomicBoolean;

public class Server {
    private final int port;
    private TerminalThread terminalThread;
    private AtomicBoolean running;
    private ServerSocketChannel canalServidorTCP;
    private CopyOnWriteArraySet<ClientInServer> clientes;

    public Server(int port) throws IOException {
        this.port = port;
        this.running = new AtomicBoolean(true);
        canalServidorTCP = ServerSocketChannel.open();
        canalServidorTCP.bind(new InetSocketAddress(this.port));
        this.clientes = new CopyOnWriteArraySet<>();

    }

    private void retransmissionMessage(ClientInServer clientInServer, String message) {
        for (ClientInServer client : clientes) {
            if (client != clientInServer) {
                client.writeMessage(message);
            }
        }
    }
    private void onMessageReceived(ClientInServer cliente, String msg) {
        ies.sequeros.psp.socketsclasicotcp.server.Command c = Command.fromDescription(msg);
        if (c != null) {
            this.processCommand(c, cliente);
        } else {
            this.retransmissionMessage(cliente, "Mensaje (" + cliente.toString() + "): " + msg);
            System.out.println("Mensaje de " + cliente.hashCode() + ": " + msg);
        }
    }
    private void processCommand(Command c, ClientInServer client) {
        switch (c) {
            case QUIT:
                System.out.println("Cliente desconectado por comando: " + c);
                //this.retransmissionMessage(client, "Cliente desconectado (" + c + ")");
                client.close();
                break;

            case LISTCLIENTS:
                StringBuilder sb = new StringBuilder("Listado de clientes:\n");
                for (ClientInServer cliente : clientes) {
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

    public void start() throws IOException {
        this.terminalThread = new TerminalThread(running, (msg) -> {
            try {
                close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
        this.terminalThread.setDaemon(true);
        this.terminalThread.start();
        System.out.println("Servidor escuchando en el puerto " + this.port + "...");

        try {
            while (running.get()) {
                SocketChannel canalClienteTCP = canalServidorTCP.accept();
                ClientInServer client = new ClientInServer(
                        canalClienteTCP,
                        running,
                        this::onClientDisconnect, this::onMessageReceived
                );
                this.clientes.add(client);
                Thread.startVirtualThread(client);
            }
        } catch (IOException e) {
            // Si running es false, la excepción es normal porque close() cerró el canal
            if (running.get()) {
                System.err.println("Error inesperado en accept(): " + e.getMessage());
            }
        }

    }
    private void onClientDisconnect(ClientInServer clientInServer) {
        try {
            clientInServer.close();
        } catch (Exception e) {
            System.err.println("Error al cerrar cliente desconectado: " + e.getMessage());
        }
        clientes.remove(clientInServer);
    }

    public void close() throws IOException {
        //solo lo ejecuta un hilo
        if (running.compareAndSet(true, false)) {

            // Cerrar el ServerSocket para desbloquear inmediatamente el accept()
            if (this.canalServidorTCP != null && this.canalServidorTCP.isOpen()) {
                this.canalServidorTCP.close();
            }

            // Cerrar a todos los clientes de forma segura e independiente
            for (ClientInServer client : this.clientes) {
                try {
                    client.close();
                } catch (Exception e) {
                    System.err.println("Error al cerrar un cliente durante el apagado: " + e.getMessage());
                }
            }
            clientes.clear();
            // Interrumpir hilo de terminal (Nota: System.in seguirá bloqueado hasta recibir un ENTER)
            if (this.terminalThread != null) {
                this.terminalThread.interrupt();
            }
            System.out.println("Servidor cerrado correctamente.");
        }
    }
    public static void main(String[] args) {
        try {
            Server servidor = new Server(5000);
            servidor.start();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
