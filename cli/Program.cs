using System;
using System.Diagnostics;
public class App
{

    public static void Main()
    {
        CreateProjectReact();
    }

    public static void CreateProjectReact()
    {
        Console.WriteLine("Enter your project name : ");
        string project_name = Console.ReadLine()!;
        string cmd_react = $"npx create-react-app {project_name}";
        string replace_module_export = "module.exports = {content: [\"./src/**/*.{js,jsx,ts,tsx}\"], theme: {extend: {},}, plugins: []}";
        string add_tailwind_nodes = "@tailwind base; @tailwind components; @tailwind utilities;";

        var list_cmd = new List<string>
            {
                cmd_react,
                $"cd {project_name}",
                "npm install -D tailwindcss",
                "npx tailwindcss init",
                replace_module_export,
                add_tailwind_nodes,
                "npm run start"
            };

        var new_list_cmd = new List<string>{
                "notepad",
            };

        foreach (string cmd in new_list_cmd)
        {
            var process = Process.Start(cmd);
            Console.WriteLine($"Opening {cmd} ...");
            process.WaitForExit();
            // Console.WriteLine(cmd);
        };

        Console.WriteLine(project_name);
    }
}