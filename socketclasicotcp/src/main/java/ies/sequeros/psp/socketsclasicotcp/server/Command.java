package ies.sequeros.psp.socketsclasicotcp.server;

public enum Command {
    QUIT("quit"),
    LISTCLIENTS("list");
    private String command;
    private Command(String command) {
        this.command = command;
    }
    public String getCommand() {
        return command;
    };
    public static Command fromDescription(String texto) {
        if (texto == null) {
            return null;
        }

        for (Command item : Command.values()) {
            if (item.command.equalsIgnoreCase(texto)) {
                return item;
            }
        }
        return null;
    }
}
