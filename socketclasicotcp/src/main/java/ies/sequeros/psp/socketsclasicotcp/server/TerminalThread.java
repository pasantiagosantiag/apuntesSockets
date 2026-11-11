package ies.sequeros.psp.socketsclasicotcp.server;

import java.util.Scanner;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

public class TerminalThread extends Thread {
    private final AtomicBoolean running;
    private final Consumer<String> onExit;

    public TerminalThread(AtomicBoolean running, Consumer<String> onExit) {
        this.running = running;
        this.onExit = onExit;
        setName("TerminalReaderThread");
        setDaemon(true); // Marca el hilo como daemon para que no bloquee el cierre de la JVM si es necesario
    }
    @Override
    public void run() {
        // Asegurar que el Scanner se cierre correctamente
        try (Scanner scanner = new Scanner(System.in)) {
            while (running.get() && scanner.hasNextLine()) {
                String linea = scanner.nextLine();
                if (linea.equalsIgnoreCase("exit") || linea.equalsIgnoreCase("quit")) {
                    //se cambia la variable compartida y provoca el cierre de todo el servidor
                    running.set(false);
                    if (onExit != null) {
                        onExit.accept(linea);
                    }
                    break;
                }
            }
        } catch (Exception e) {
            // Si el flujo de entrada se cierra abruptamente o se interrumpe
            if (running.get()) {
                System.err.println("Terminal cerrada o interrumpida: " + e.getMessage());
            }
        }
    }
}