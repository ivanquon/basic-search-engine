async function search(query) {
  const response = await fetch(`http://localhost:8000/retrieval/${query}`);
  const data = await response.json();
  document.getElementById(
    "time"
  ).innerText = `Query finished in ${data.mstime} ms`;
  document.getElementById("urls").innerHTML = data.urls
    .map((url) => `<li><a href=${url[0]}>${url[0]}</a></li>`)
    .join("");
  console.log(data);
}

document.getElementById("search").addEventListener("submit", async (e) => {
  e.preventDefault();

  const query = document.getElementById("query").value;
  if (!query.trim()) return;

  await search(query);
});
