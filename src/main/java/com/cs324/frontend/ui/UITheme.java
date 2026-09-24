package com.cs324.frontend.ui;

import javax.swing.BorderFactory;
import javax.swing.DefaultListCellRenderer;
import javax.swing.JButton;
import javax.swing.JComboBox;
import javax.swing.JComponent;
import javax.swing.JLabel;
import javax.swing.JList;
import javax.swing.JScrollPane;
import javax.swing.JTable;
import javax.swing.JTextArea;
import javax.swing.JTextField;
import javax.swing.UIManager;
import javax.swing.border.Border;
import javax.swing.table.JTableHeader;
import java.awt.Color;
import java.awt.Component;
import java.awt.Cursor;
import java.awt.Font;

/**
 * Shared dark-navy look-and-feel used by both the server and client GUIs.
 * Provides the colour palette, the larger fonts and the component styling so
 * the two GUIs stay visually consistent.
 */
public final class UITheme {

    public static final Color BACKGROUND = new Color(0x0E, 0x16, 0x2A);
    public static final Color PANEL = new Color(0x14, 0x24, 0x3B);
    public static final Color CARD = new Color(0x1B, 0x30, 0x4E);
    public static final Color INPUT = new Color(0x0A, 0x12, 0x20);
    public static final Color ACCENT = new Color(0x52, 0xAA, 0xFF);
    public static final Color BUTTON = new Color(0x2A, 0x52, 0x86);
    public static final Color BUTTON_ACTIVE = new Color(0x1E, 0x3B, 0x5F);
    public static final Color TEXT = new Color(0xEA, 0xF2, 0xFC);
    public static final Color MUTED = new Color(0x9D, 0xAF, 0xC4);
    public static final Color SUCCESS = new Color(0x3F, 0xD9, 0x8B);
    public static final Color WARNING = new Color(0xFF, 0xC2, 0x3E);
    public static final Color ERROR = new Color(0xFF, 0x5A, 0x5A);
    public static final Color BORDER = new Color(0x33, 0x4D, 0x70);

    public static final Font BASE = new Font("SansSerif", Font.PLAIN, 15);
    public static final Font MEDIUM = new Font("SansSerif", Font.PLAIN, 17);
    public static final Font BOLD = new Font("SansSerif", Font.BOLD, 15);
    public static final Font SECTION = new Font("SansSerif", Font.BOLD, 17);
    public static final Font TITLE = new Font("SansSerif", Font.BOLD, 21);
    public static final Font BIG = new Font("SansSerif", Font.BOLD, 28);
    public static final Font MONO = new Font(Font.MONOSPACED, Font.PLAIN, 14);

    private UITheme() {
    }

    /** Sets sensible defaults so popup dialogs and standard widgets inherit the theme. */
    public static void install() {
        try {
            UIManager.setLookAndFeel(UIManager.getCrossPlatformLookAndFeelClassName());
        } catch (Exception ignored) {
            // fall back to the platform look-and-feel
        }
        UIManager.put("Panel.background", BACKGROUND);
        UIManager.put("OptionPane.background", PANEL);
        UIManager.put("OptionPane.messageForeground", TEXT);
        UIManager.put("OptionPane.messageFont", MEDIUM);
        UIManager.put("OptionPane.buttonFont", BOLD);
        UIManager.put("OptionPane.foreground", TEXT);
        UIManager.put("Table.alternateRowColor", CARD);
        UIManager.put("Table.gridColor", BORDER);
        UIManager.put("ScrollPane.background", BACKGROUND);
        UIManager.put("List.background", INPUT);
        UIManager.put("List.foreground", TEXT);
        UIManager.put("ComboBox.background", INPUT);
        UIManager.put("ComboBox.foreground", TEXT);
        UIManager.put("ComboBox.selectionBackground", BUTTON);
        UIManager.put("ComboBox.selectionForeground", TEXT);
    }

    public static Border titledBorder(String title) {
        return BorderFactory.createTitledBorder(
                BorderFactory.createLineBorder(BORDER),
                title,
                javax.swing.border.TitledBorder.LEFT,
                javax.swing.border.TitledBorder.DEFAULT_POSITION,
                SECTION,
                ACCENT);
    }

    public static void panel(JComponent component) {
        component.setBackground(PANEL);
    }

    public static void label(JLabel label, Color color) {
        label.setFont(MEDIUM);
        label.setForeground(color == null ? TEXT : color);
    }

    public static void button(JButton button) {
        button.setFont(BOLD);
        button.setBackground(BUTTON);
        button.setForeground(TEXT);
        button.setFocusPainted(false);
        button.setOpaque(true);
        button.setCursor(Cursor.getPredefinedCursor(Cursor.HAND_CURSOR));
        button.setBorder(BorderFactory.createCompoundBorder(
                BorderFactory.createLineBorder(BORDER),
                BorderFactory.createEmptyBorder(9, 16, 9, 16)));
    }

    public static void textField(JTextField field) {
        field.setBackground(INPUT);
        field.setForeground(TEXT);
        field.setCaretColor(TEXT);
        field.setFont(MEDIUM);
        field.setBorder(BorderFactory.createCompoundBorder(
                BorderFactory.createLineBorder(BORDER),
                BorderFactory.createEmptyBorder(6, 8, 6, 8)));
    }

    public static void textArea(JTextArea area) {
        area.setBackground(INPUT);
        area.setForeground(TEXT);
        area.setCaretColor(TEXT);
        area.setFont(MEDIUM);
        area.setBorder(BorderFactory.createCompoundBorder(
                BorderFactory.createLineBorder(BORDER),
                BorderFactory.createEmptyBorder(6, 8, 6, 8)));
    }

    public static void combo(JComboBox<?> combo) {
        combo.setBackground(INPUT);
        combo.setForeground(TEXT);
        combo.setFont(MEDIUM);
        combo.setRenderer(new DefaultListCellRenderer() {
            @Override
            public Component getListCellRendererComponent(JList<?> list, Object value, int index,
                                                          boolean isSelected, boolean cellHasFocus) {
                super.getListCellRendererComponent(list, value, index, isSelected, cellHasFocus);
                setFont(MEDIUM);
                setBorder(BorderFactory.createEmptyBorder(6, 10, 6, 10));
                setBackground(isSelected ? BUTTON : INPUT);
                setForeground(TEXT);
                return this;
            }
        });
    }

    public static void table(JTable table) {
        table.setBackground(CARD);
        table.setForeground(TEXT);
        table.setSelectionBackground(BUTTON);
        table.setSelectionForeground(TEXT);
        table.setFont(BASE);
        table.setRowHeight((int) (BASE.getSize() * 2.0));
        table.setShowGrid(true);
        table.setGridColor(BORDER);
        table.setFillsViewportHeight(true);
        JTableHeader header = table.getTableHeader();
        header.setBackground(BUTTON);
        header.setForeground(TEXT);
        header.setFont(SECTION);
        header.setReorderingAllowed(false);
    }

    public static void scroll(JScrollPane scroll) {
        scroll.setBorder(BorderFactory.createLineBorder(BORDER));
        scroll.getViewport().setBackground(BACKGROUND);
    }
}