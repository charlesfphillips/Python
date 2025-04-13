import tkinter as tk
from tkinter import ttk, filedialog, messagebox, scrolledtext
import subprocess
import os
import sys
import threading
import queue
import re
import json
import time
from typing import List, Dict, Any, Optional, Tuple

class DatabaseDocumenterGUI:
    def __init__(self, root):
        self.root = root
        self.root.title("Database Schema Documenter")
        self.root.geometry("800x700")
        self.root.minsize(800, 700)
        
        # Set icon if available
        try:
            self.root.iconbitmap("db_icon.ico")
        except:
            pass
        
        # Create a queue for output
        self.output_queue = queue.Queue()
        
        # Create main frame
        self.main_frame = ttk.Frame(root, padding="10")
        self.main_frame.pack(fill=tk.BOTH, expand=True)
        
        # Create notebook for tabs
        self.notebook = ttk.Notebook(self.main_frame)
        self.notebook.pack(fill=tk.BOTH, expand=True, pady=5)
        
        # Create tabs
        self.connection_tab = ttk.Frame(self.notebook)
        self.output_tab = ttk.Frame(self.notebook)
        self.settings_tab = ttk.Frame(self.notebook)
        self.about_tab = ttk.Frame(self.notebook)
        
        self.notebook.add(self.connection_tab, text="Connection")
        self.notebook.add(self.output_tab, text="Output")
        self.notebook.add(self.settings_tab, text="Settings")
        self.notebook.add(self.about_tab, text="About")
        
        # Setup connection tab
        self._setup_connection_tab()
        
        # Setup output tab
        self._setup_output_tab()
        
        # Setup settings tab
        self._setup_settings_tab()
        
        # Setup about tab
        self._setup_about_tab()
        
        # Setup bottom buttons
        self._setup_bottom_buttons()
        
        # Start the output consumer
        self.running = True
        self.output_consumer_thread = threading.Thread(target=self._consume_output, daemon=True)
        self.output_consumer_thread.start()
        
        # Bind close event
        self.root.protocol("WM_DELETE_WINDOW", self._on_close)
        
        # Load settings if available
        self._load_settings()

    def _setup_connection_tab(self):
        """Setup the connection tab with database connection options"""
        # Create frames
        connection_frame = ttk.LabelFrame(self.connection_tab, text="Database Connection", padding="10")
        connection_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)
        
        # Database type selection
        db_type_frame = ttk.Frame(connection_frame)
        db_type_frame.pack(fill=tk.X, pady=5)
        
        ttk.Label(db_type_frame, text="Database Type:").pack(side=tk.LEFT, padx=5)
        self.db_type_var = tk.StringVar(value="mssql")
        self.db_type_combo = ttk.Combobox(db_type_frame, textvariable=self.db_type_var, 
                                          values=["mssql", "db2"], state="readonly", width=10)
        self.db_type_combo.pack(side=tk.LEFT, padx=5)
        self.db_type_combo.bind("<<ComboboxSelected>>", self._on_db_type_change)
        
        # Connection string frame
        conn_string_frame = ttk.Frame(connection_frame)
        conn_string_frame.pack(fill=tk.X, pady=5)
        
        self.conn_string_label = ttk.Label(conn_string_frame, text="Connection String:")
        self.conn_string_label.pack(side=tk.LEFT, padx=5)
        
        self.conn_string_var = tk.StringVar()
        self.conn_string_entry = ttk.Entry(conn_string_frame, textvariable=self.conn_string_var, width=60)
        self.conn_string_entry.pack(side=tk.LEFT, padx=5, fill=tk.X, expand=True)
        
        # JDBC URL frame (for DB2)
        self.jdbc_frame = ttk.Frame(connection_frame)
        self.jdbc_frame.pack(fill=tk.X, pady=5)
        self.jdbc_frame.pack_forget()  # Hide initially
        
        ttk.Label(self.jdbc_frame, text="JDBC URL:").pack(side=tk.LEFT, padx=5)
        self.jdbc_url_var = tk.StringVar()
        self.jdbc_url_entry = ttk.Entry(self.jdbc_frame, textvariable=self.jdbc_url_var, width=60)
        self.jdbc_url_entry.pack(side=tk.LEFT, padx=5, fill=tk.X, expand=True)
        
        # Schema frame (for DB2)
        self.schema_frame = ttk.Frame(connection_frame)
        self.schema_frame.pack(fill=tk.X, pady=5)
        self.schema_frame.pack_forget()  # Hide initially
        
        ttk.Label(self.schema_frame, text="Schema:").pack(side=tk.LEFT, padx=5)
        self.schema_var = tk.StringVar()
        self.schema_entry = ttk.Entry(self.schema_frame, textvariable=self.schema_var, width=30)
        self.schema_entry.pack(side=tk.LEFT, padx=5)
        
        # Credentials frame
        creds_frame = ttk.Frame(connection_frame)
        creds_frame.pack(fill=tk.X, pady=5)
        
        ttk.Label(creds_frame, text="Username:").pack(side=tk.LEFT, padx=5)
        self.username_var = tk.StringVar()
        self.username_entry = ttk.Entry(creds_frame, textvariable=self.username_var, width=20)
        self.username_entry.pack(side=tk.LEFT, padx=5)
        
        ttk.Label(creds_frame, text="Password:").pack(side=tk.LEFT, padx=5)
        self.password_var = tk.StringVar()
        self.password_entry = ttk.Entry(creds_frame, textvariable=self.password_var, width=20, show="*")
        self.password_entry.pack(side=tk.LEFT, padx=5)
        
        # Output options frame
        output_frame = ttk.LabelFrame(self.connection_tab, text="Output Options", padding="10")
        output_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)
        
        # Output format
        format_frame = ttk.Frame(output_frame)
        format_frame.pack(fill=tk.X, pady=5)
        
        ttk.Label(format_frame, text="Output Format:").pack(side=tk.LEFT, padx=5)
        self.output_format_var = tk.StringVar(value="all")
        self.output_format_combo = ttk.Combobox(format_frame, textvariable=self.output_format_var, 
                                               values=["all", "markdown", "excel", "csv", "json"], 
                                               state="readonly", width=10)
        self.output_format_combo.pack(side=tk.LEFT, padx=5)
        
        # Output directory
        dir_frame = ttk.Frame(output_frame)
        dir_frame.pack(fill=tk.X, pady=5)
        
        ttk.Label(dir_frame, text="Output Directory:").pack(side=tk.LEFT, padx=5)
        self.output_dir_var = tk.StringVar(value=os.path.join(os.path.expanduser("~"), "Documents", "db_documentation"))
        self.output_dir_entry = ttk.Entry(dir_frame, textvariable=self.output_dir_var, width=50)
        self.output_dir_entry.pack(side=tk.LEFT, padx=5, fill=tk.X, expand=True)
        
        self.browse_button = ttk.Button(dir_frame, text="Browse...", command=self._browse_output_dir)
        self.browse_button.pack(side=tk.LEFT, padx=5)
        
        # ERD options
        erd_frame = ttk.Frame(output_frame)
        erd_frame.pack(fill=tk.X, pady=5)
        
        self.generate_erd_var = tk.BooleanVar(value=True)
        self.generate_erd_check = ttk.Checkbutton(erd_frame, text="Generate Entity Relationship Diagram", 
                                                 variable=self.generate_erd_var)
        self.generate_erd_check.pack(side=tk.LEFT, padx=5)
        
        # Graphviz path
        graphviz_frame = ttk.Frame(output_frame)
        graphviz_frame.pack(fill=tk.X, pady=5)
        
        ttk.Label(graphviz_frame, text="Graphviz Path:").pack(side=tk.LEFT, padx=5)
        self.graphviz_path_var = tk.StringVar()
        self.graphviz_path_entry = ttk.Entry(graphviz_frame, textvariable=self.graphviz_path_var, width=50)
        self.graphviz_path_entry.pack(side=tk.LEFT, padx=5, fill=tk.X, expand=True)
        
        self.graphviz_browse_button = ttk.Button(graphviz_frame, text="Browse...", 
                                                command=self._browse_graphviz_path)
        self.graphviz_browse_button.pack(side=tk.LEFT, padx=5)
        
        # Set default values
        self._set_default_connection_string()

    def _on_db_type_change(self, event=None):
        """Handle database type change"""
        db_type = self.db_type_var.get()
        
        if db_type == "db2":
            # Show JDBC URL and schema fields for DB2
            self.jdbc_frame.pack(fill=tk.X, pady=5, after=self.conn_string_entry.master)
            self.schema_frame.pack(fill=tk.X, pady=5, after=self.jdbc_frame)
            self.conn_string_label.config(text="Connection String (Optional):")
        else:
            # Hide JDBC URL and schema fields for other database types
            self.jdbc_frame.pack_forget()
            self.schema_frame.pack_forget()
            self.conn_string_label.config(text="Connection String:")
        
        # Update default connection string
        self._set_default_connection_string()

    def _set_default_connection_string(self):
        """Set default connection string based on database type"""
        db_type = self.db_type_var.get()
        
        if db_type == "mssql":
            self.conn_string_var.set("DRIVER={SQL Server};SERVER=localhost;DATABASE=master;Trusted_Connection=yes;")
        elif db_type == "db2":
            self.conn_string_var.set("DRIVER={IBM DB2 ODBC DRIVER};DATABASE=sample;HOSTNAME=localhost;PORT=50000;PROTOCOL=TCPIP;UID=db2admin;PWD=password;")
            self.jdbc_url_var.set("jdbc:db2://localhost:50000/sample")

    def _setup_output_tab(self):
        """Setup the output tab with console output"""
        output_frame = ttk.Frame(self.output_tab, padding="10")
        output_frame.pack(fill=tk.BOTH, expand=True)
        
        # Output text area
        self.output_text = scrolledtext.ScrolledText(output_frame, wrap=tk.WORD, height=20, width=80)
        self.output_text.pack(fill=tk.BOTH, expand=True)
        self.output_text.config(state=tk.DISABLED)
        
        # Clear button
        self.clear_button = ttk.Button(output_frame, text="Clear Output", command=self._clear_output)
        self.clear_button.pack(pady=5)

    def _setup_settings_tab(self):
        """Setup the settings tab"""
        settings_frame = ttk.Frame(self.settings_tab, padding="10")
        settings_frame.pack(fill=tk.BOTH, expand=True)
        
        # Save settings
        save_settings_frame = ttk.Frame(settings_frame)
        save_settings_frame.pack(fill=tk.X, pady=5)
        
        self.save_settings_var = tk.BooleanVar(value=True)
        self.save_settings_check = ttk.Checkbutton(save_settings_frame, 
                                                  text="Save settings between sessions", 
                                                  variable=self.save_settings_var)
        self.save_settings_check.pack(side=tk.LEFT, padx=5)
        
        # Advanced options
        advanced_frame = ttk.LabelFrame(settings_frame, text="Advanced Options", padding="10")
        advanced_frame.pack(fill=tk.BOTH, expand=True, pady=10)
        
        # Python executable
        python_frame = ttk.Frame(advanced_frame)
        python_frame.pack(fill=tk.X, pady=5)
        
        ttk.Label(python_frame, text="Python Executable:").pack(side=tk.LEFT, padx=5)
        self.python_path_var = tk.StringVar(value=sys.executable)
        self.python_path_entry = ttk.Entry(python_frame, textvariable=self.python_path_var, width=50)
        self.python_path_entry.pack(side=tk.LEFT, padx=5, fill=tk.X, expand=True)
        
        self.python_browse_button = ttk.Button(python_frame, text="Browse...", 
                                              command=self._browse_python_path)
        self.python_browse_button.pack(side=tk.LEFT, padx=5)
        
        # Script path
        script_frame = ttk.Frame(advanced_frame)
        script_frame.pack(fill=tk.X, pady=5)
        
        ttk.Label(script_frame, text="Script Path:").pack(side=tk.LEFT, padx=5)
        script_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "db_documenter.py")
        self.script_path_var = tk.StringVar(value=script_path)
        self.script_path_entry = ttk.Entry(script_frame, textvariable=self.script_path_var, width=50)
        self.script_path_entry.pack(side=tk.LEFT, padx=5, fill=tk.X, expand=True)
        
        self.script_browse_button = ttk.Button(script_frame, text="Browse...", 
                                              command=self._browse_script_path)
        self.script_browse_button.pack(side=tk.LEFT, padx=5)

    def _setup_about_tab(self):
        """Setup the about tab with information about the tool"""
        about_frame = ttk.Frame(self.about_tab, padding="20")
        about_frame.pack(fill=tk.BOTH, expand=True)
        
        # Title
        title_label = ttk.Label(about_frame, text="Database Schema Documenter", 
                               font=("TkDefaultFont", 14, "bold"))
        title_label.pack(pady=10)
        
        # Version
        version_label = ttk.Label(about_frame, text="Version 1.0.0")
        version_label.pack()
        
        # Description
        description = (
            "This tool extracts and documents database schemas from Microsoft SQL Server "
            "and IBM DB2 databases. It generates documentation in various formats including "
            "Markdown, Excel, CSV, and JSON. It can also generate entity relationship diagrams."
        )
        desc_label = ttk.Label(about_frame, text=description, wraplength=600, justify="center")
        desc_label.pack(pady=10)
        
        # Requirements
        req_frame = ttk.LabelFrame(about_frame, text="Requirements", padding="10")
        req_frame.pack(fill=tk.X, pady=10)
        
        requirements = (
            "• Python 3.6 or higher\n"
            "• Required Python packages: pyodbc, pandas, openpyxl, pydot\n"
            "• For DB2 connections: ibm_db, ibm_db_dbi, or jaydebeapi with DB2 JDBC driver\n"
            "• For ERD generation: Graphviz (https://graphviz.org/download/)"
        )
        req_label = ttk.Label(req_frame, text=requirements, justify="left")
        req_label.pack(pady=5)
        
        # Author
        author_label = ttk.Label(about_frame, text="Created by Your Name")
        author_label.pack(pady=10)

    def _setup_bottom_buttons(self):
        """Setup the bottom buttons"""
        button_frame = ttk.Frame(self.main_frame)
        button_frame.pack(fill=tk.X, pady=10)
        
        # Run button
        self.run_button = ttk.Button(button_frame, text="Run", command=self._run_documenter)
        self.run_button.pack(side=tk.RIGHT, padx=5)
        
        # Save settings button
        self.save_button = ttk.Button(button_frame, text="Save Settings", command=self._save_settings)
        self.save_button.pack(side=tk.RIGHT, padx=5)
        
        # Exit button
        self.exit_button = ttk.Button(button_frame, text="Exit", command=self._on_close)
        self.exit_button.pack(side=tk.RIGHT, padx=5)

    def _browse_output_dir(self):
        """Browse for output directory"""
        directory = filedialog.askdirectory(initialdir=self.output_dir_var.get())
        if directory:
            self.output_dir_var.set(directory)

    def _browse_graphviz_path(self):
        """Browse for Graphviz bin directory"""
        directory = filedialog.askdirectory(title="Select Graphviz bin directory")
        if directory:
            self.graphviz_path_var.set(directory)

    def _browse_python_path(self):
        """Browse for Python executable"""
        file_path = filedialog.askopenfilename(
            title="Select Python Executable",
            filetypes=[("Python Executable", "python*.exe"), ("All Files", "*.*")]
        )
        if file_path:
            self.python_path_var.set(file_path)

    def _browse_script_path(self):
        """Browse for script path"""
        file_path = filedialog.askopenfilename(
            title="Select Script File",
            filetypes=[("Python Files", "*.py"), ("All Files", "*.*")]
        )
        if file_path:
            self.script_path_var.set(file_path)

    def _clear_output(self):
        """Clear the output text area"""
        self.output_text.config(state=tk.NORMAL)
        self.output_text.delete(1.0, tk.END)
        self.output_text.config(state=tk.DISABLED)

    def _run_documenter(self):
        """Run the database documenter script"""
        # Disable the run button to prevent multiple runs
        self.run_button.config(state=tk.DISABLED)
        
        # Get values from UI
        db_type = self.db_type_var.get()
        conn_string = self.conn_string_var.get()
        username = self.username_var.get()
        password = self.password_var.get()
        output_format = self.output_format_var.get()
        output_dir = self.output_dir_var.get()
        generate_erd = self.generate_erd_var.get()
        graphviz_path = self.graphviz_path_var.get()
        python_path = self.python_path_var.get()
        script_path = self.script_path_var.get()
        jdbc_url = self.jdbc_url_var.get() if db_type == "db2" else ""
        schema = self.schema_var.get() if db_type == "db2" else ""
        
        # Validate inputs
        if db_type == "mssql" and not conn_string:
            messagebox.showerror("Error", "Connection string is required")
            self.run_button.config(state=tk.NORMAL)
            return
        
        if db_type == "db2" and not (conn_string or jdbc_url):
            messagebox.showerror("Error", "Either connection string or JDBC URL is required for DB2")
            self.run_button.config(state=tk.NORMAL)
            return
        
        if not output_dir:
            messagebox.showerror("Error", "Output directory is required")
            self.run_button.config(state=tk.NORMAL)
            return
        
        # Create output directory if it doesn't exist
        if not os.path.exists(output_dir):
            try:
                os.makedirs(output_dir)
            except Exception as e:
                messagebox.showerror("Error", f"Failed to create output directory: {str(e)}")
                self.run_button.config(state=tk.NORMAL)
                return
        
        # Build command
        cmd = [python_path, script_path, "--db-type", db_type]
        
        if conn_string:
            cmd.extend(["--connection-string", conn_string])
        
        if db_type == "db2" and jdbc_url:
            cmd.extend(["--jdbc-url", jdbc_url])
            
        if db_type == "db2" and schema:
            cmd.extend(["--schema", schema])
        
        if username:
            cmd.extend(["--username", username])
        
        if password:
            cmd.extend(["--password", password])
        
        cmd.extend(["--output-format", output_format])
        cmd.extend(["--output-dir", output_dir])
        
        if generate_erd:
            cmd.append("--erd")
        
        if graphviz_path:
            cmd.extend(["--graphviz-path", graphviz_path])
        
        # Log the command (hide password)
        log_cmd = cmd.copy()
        if "--password" in log_cmd:
            pwd_index = log_cmd.index("--password")
            if pwd_index + 1 < len(log_cmd):
                log_cmd[pwd_index + 1] = "'********'"
        
        self._write_to_output(f"Running command: {' '.join(log_cmd)}")
        
        # Run the command in a separate thread
        threading.Thread(target=self._execute_command, args=(cmd,), daemon=True).start()

    def _execute_command(self, cmd):
        """Execute the command and capture output"""
        try:
            # Create a process and capture output
            process = subprocess.Popen(
                cmd, 
                stdout=subprocess.PIPE, 
                stderr=subprocess.STDOUT,
                universal_newlines=True,
                bufsize=1
            )
            
            # Read output line by line
            for line in iter(process.stdout.readline, ''):
                self.output_queue.put(line)
            
            # Wait for process to complete
            return_code = process.wait()
            
            if return_code == 0:
                self.output_queue.put("Command completed successfully!")
            else:
                self.output_queue.put(f"Command failed with return code {return_code}")
        except Exception as e:
            self.output_queue.put(f"Error executing command: {str(e)}")
            
            # Get traceback
            import traceback
            self.output_queue.put(traceback.format_exc())
        finally:
            # Re-enable the run button
            self.root.after(0, lambda: self.run_button.config(state=tk.NORMAL))

    def _consume_output(self):
        """Consume output from the queue and write to the output text area"""
        while self.running:
            try:
                # Get output from queue with timeout
                line = self.output_queue.get(timeout=0.1)
                
                # Write to output text area
                self.root.after(0, lambda l=line: self._write_to_output(l))
                
                # Mark task as done
                self.output_queue.task_done()
            except queue.Empty:
                # No output available, sleep briefly
                time.sleep(0.1)
            except Exception as e:
                print(f"Error in output consumer: {str(e)}")

    def _write_to_output(self, text):
        """Write text to the output text area"""
        self.output_text.config(state=tk.NORMAL)
        self.output_text.insert(tk.END, text + "\n")
        self.output_text.see(tk.END)
        self.output_text.config(state=tk.DISABLED)

    def _save_settings(self):
        """Save current settings to a JSON file"""
        settings = {
            "db_type": self.db_type_var.get(),
            "conn_string": self.conn_string_var.get(),
            "jdbc_url": self.jdbc_url_var.get(),
            "schema": self.schema_var.get(),
            "username": self.username_var.get(),
            "output_format": self.output_format_var.get(),
            "output_dir": self.output_dir_var.get(),
            "generate_erd": self.generate_erd_var.get(),
            "graphviz_path": self.graphviz_path_var.get(),
            "python_path": self.python_path_var.get(),
            "script_path": self.script_path_var.get(),
            "save_settings": self.save_settings_var.get()
        }
        
        # Don't save password for security reasons
        
        try:
            settings_dir = os.path.join(os.path.expanduser("~"), ".db_documenter")
            if not os.path.exists(settings_dir):
                os.makedirs(settings_dir)
                
            settings_file = os.path.join(settings_dir, "settings.json")
            
            with open(settings_file, "w") as f:
                json.dump(settings, f, indent=4)
                
            messagebox.showinfo("Settings Saved", "Settings have been saved successfully.")
        except Exception as e:
            messagebox.showerror("Error", f"Failed to save settings: {str(e)}")

    def _load_settings(self):
        """Load settings from JSON file"""
        try:
            settings_file = os.path.join(os.path.expanduser("~"), ".db_documenter", "settings.json")
            
            if os.path.exists(settings_file):
                with open(settings_file, "r") as f:
                    settings = json.load(f)
                
                # Apply settings
                if "db_type" in settings:
                    self.db_type_var.set(settings["db_type"])
                    # Trigger the change event manually
                    self._on_db_type_change()
                
                if "conn_string" in settings:
                    self.conn_string_var.set(settings["conn_string"])
                
                if "jdbc_url" in settings:
                    self.jdbc_url_var.set(settings["jdbc_url"])
                    
                if "schema" in settings:
                    self.schema_var.set(settings["schema"])
                
                if "username" in settings:
                    self.username_var.set(settings["username"])
                
                if "output_format" in settings:
                    self.output_format_var.set(settings["output_format"])
                
                if "output_dir" in settings:
                    self.output_dir_var.set(settings["output_dir"])
                
                if "generate_erd" in settings:
                    self.generate_erd_var.set(settings["generate_erd"])
                
                if "graphviz_path" in settings:
                    self.graphviz_path_var.set(settings["graphviz_path"])
                
                if "python_path" in settings:
                    self.python_path_var.set(settings["python_path"])
                
                if "script_path" in settings:
                    self.script_path_var.set(settings["script_path"])
                
                if "save_settings" in settings:
                    self.save_settings_var.set(settings["save_settings"])
        except Exception as e:
            print(f"Error loading settings: {str(e)}")

    def _on_close(self):
        """Handle window close event"""
        # Save settings if enabled
        if self.save_settings_var.get():
            self._save_settings()
            
        # Stop the output consumer thread
        self.running = False
        if self.output_consumer_thread.is_alive():
            self.output_consumer_thread.join(1.0)  # Wait up to 1 second
            
        # Close the window
        self.root.destroy()

def main():
    root = tk.Tk()
    app = DatabaseDocumenterGUI(root)
    root.mainloop()

if __name__ == "__main__":
    main()
