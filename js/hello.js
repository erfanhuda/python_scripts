const { fork, exec, execFile, spawn } = require("child_process");
const { Interface } = require("readline");

class Windows {
  constructor(self) {
    this._variable = self;
  }

  exec() {
    exec("dir", (err, stdout, stderr) => {
      if (err) {
        console.log(`error: ${err.message}`);
        return;
      }
      if (stderr) {
        console.log(`stderr : ${stderr.message}`);
        return;
      }
      console.log(`stdout : ${stdout}`);
    });
  }

  execFile() {
    execFile("test.bat", (err, stdout, stderr) => {
      if (err) {
        console.log(`error: ${err.message}`);
        return;
      }
      if (stderr) {
        console.log(`stderr : ${stderr}`);
        return;
      }
      console.log(`stdout : ${stdout}`);
    });
  }
  spawn() {
    const bat = spawn("cmd.exe", ["/c", "python", "find_network.py"]);

    bat.stdout.on("data", (data) => {
      console.log(data.toString());
    });

    bat.stderr.on("data", (data) => {
      console.error(data.toString());
    });

    bat.on("exit", (code) => {
      console.log(`Child exited with code ${code}`);
    });
  }
}

win = new Windows();
win.execFile();
