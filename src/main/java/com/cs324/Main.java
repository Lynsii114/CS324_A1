package com.cs324;

import com.cs324.backend.bootstrap.BootstrapServer;
import com.cs324.frontend.client.DistriLabClientGui;

/**
 * Single-click application entry point.
 *
 * <p>Starts the Bootstrap Node, then opens the Client GUI. Workers are still
 * started one by one from the GUI so the assignment demo flow remains intact.
 */
public class Main {
    public static void main(String[] args) {
        BootstrapServer.main(new String[0]);
        DistriLabClientGui.main(new String[] {"Client 1"});
    }
}
