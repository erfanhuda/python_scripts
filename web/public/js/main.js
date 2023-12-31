let wrapper = document.querySelector("#wrapper");
let header = wrapper.querySelector("#header");

ismouseDown = false;

let offsetX = 0;
let offsetY = 0;

function openWindow() {
  let state = wrapper.classList.contains("hidden");

  // console.log(state);
  if (state) {
    wrapper.classList.remove("hidden");
  } else {
    wrapper.classList.add("hidden");
  }
}

header.addEventListener("mousedown", (e) => {
  ismouseDown = true;
  offsetX = wrapper.offsetLeft - e.clientX;
  offsetY = wrapper.offsetTop - e.clientY;
  console.log(offsetX, offsetY);
});

document.addEventListener("mousemove", (e) => {
  if (!ismouseDown) return;
  e.preventDefault();
  let left = e.clientX + offsetX;
  let top = e.clientY + offsetY;

  wrapper.classList.remove("left-0");
  wrapper.classList.remove("top-0");
  wrapper.classList.remove("right-0");
  wrapper.classList.remove("bottom-0");
  wrapper.style.left = left + "px";
  wrapper.style.top = top + "px";
});

document.addEventListener("mouseup", () => {
  ismouseDown = false;
});
