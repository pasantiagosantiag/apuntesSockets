package ies.sequeros.psp.socketsclasicotcp.client;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.Scanner;
import java.util.concurrent.atomic.AtomicBoolean;


public class Cliente {
    private final String serverAddress;
    private final int serverPort;
    private final AtomicBoolean running = new AtomicBoolean(false);

    private Socket socket;
    private BufferedReader in;
    private PrintWriter out;

    private Thread terminalThread;
    private Thread readThread;

    public Cliente(String serverAddress, int serverPort) {
        this.serverAddress = serverAddress;
        this.serverPort = serverPort;
    }


    public void start() {
        try {
            //  Conexión  al servidor remoto
            this.socket = new Socket();
            // Establecemos un tiempo de espera de conexión (timeout de 5 segundos)
            this.socket.connect(new InetSocketAddress(serverAddress, serverPort), 5000);

            //Inicialización de flujos de E/S
            this.in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            this.out = new PrintWriter(socket.getOutputStream(), true);

            // Activar el estado de ejecución
            this.running.set(true);

            //Hilo para leer la consola y enviar mensajes al servidor
            this.terminalThread = new Thread(() -> {
                try (Scanner sc = new Scanner(System.in)) {
                    while (running.get() && sc.hasNextLine()) {
                        String line = sc.nextLine();
                        if (line.equalsIgnoreCase("quit")) {
                            stop();
                            break;
                        } else {
                            if (out != null) {
                                out.println(line);
                            }
                        }
                    }
                } catch (Exception e) {
                    if (running.get()) {
                        System.err.println("Error en la lectura de terminal: " + e.getMessage());
                    }
                }
            }, "ClientTerminal");

            // Hilo para escuchar los mensajes del servidor (Bloqueante con readLine, sin consumo de CPU)
            this.readThread = new Thread(() -> {
                try {
                    String message;
                    while (running.get() && (message = in.readLine()) != null) {
                        System.out.println(message);
                    }
                } catch (IOException e) {
                    // Si se cierra el socket intencionadamente, la excepción es esperada
                    if (running.get()) {
                        System.err.println("Conexión perdida con el servidor: " + e.getMessage());
                    }
                } finally {
                    stop();
                }
            }, "ClientReader");

            // Arrancar hilos
            this.terminalThread.start();
            this.readThread.start();

            System.out.println("Conectado al servidor en " + serverAddress + ":" + serverPort);
            System.out.println("Escribe tus mensajes (escribe 'quit' para salir):");

        } catch (IOException e) {
            System.err.println("No se pudo conectar al servidor: " + e.getMessage());
            stop();
        }
    }
    public void stop() {
        // Evitar bucles de cierre si ya estaba cerrado
        if (this.running.compareAndSet(true, false)) {
            try {
                if (socket != null && !socket.isClosed()) {
                    socket.close(); // Desbloqueará inmediatamente el in.readLine()
                }
            } catch (IOException e) {
                System.err.println("Error al cerrar el socket del cliente: " + e.getMessage());
            }
            // Interrumpir hilos por si acaso
            if (terminalThread != null) terminalThread.interrupt();
            if (readThread != null) readThread.interrupt();
            System.out.println("Cliente desconectado.");
        }
    }
    public static void main(String[] args) {
        // Validación de argumentos: se esperan exactamente 2 parámetros (IP y Puerto)
        if (args.length != 2) {
            System.err.println("Uso incorrecto.");
            System.err.println("Ejemplo de uso: java Cliente <direccion_ip> <puerto>");
            System.err.println("Ejemplo local:  java Cliente 127.0.0.1 5000");
            System.exit(1);
        }
        String ip = args[0];
        int port = 5000;
        // Validar formato de IP básica
        if (ip == null || ip.isEmpty()) {
            System.err.println("Error: La dirección IP no puede estar vacía.");
            System.exit(1);
        }
        // Validar que el puerto sea un número entero válido y esté en rango
        try {
            port = Integer.parseInt(args[1]);
            if (port < 1 || port > 65535) {
                System.err.println("Error: El puerto debe estar en el rango de 1 a 65535.");
                System.exit(1);
            }
        } catch (NumberFormatException e) {
            System.err.println("Error: El puerto especificado no es un número válido ('" + args[1] + "').");
            System.exit(1);
        }
        // Si las validaciones pasan, se arranca el cliente con los parámetros proporcionados
        Cliente cliente = new Cliente(ip, port);
        cliente.start();
    }
}