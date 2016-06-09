<!doctype html>
<html>

  <head>
    <title>Movie List - The Movie Graph</title>
    <link rel="stylesheet" href="/css/main.css">
  </head>

  <body>

    <div class="header">
      <nav><a href="/">The Movie Graph</a> / <strong>Movies</strong></nav>
    </div>

    <h1>Movies</h1>
    <ul>
    % for title, released in movies:
        <li><a href="/movie/{{title}}">{{title}} [{{released}}]</a></li>
    % end
    </ul>

    <div class="footer">
      <code>(graphs)-[:ARE]->(everywhere)</code>
      <p>With &hearts; from Sweden &amp; the <a href="http://neo4j.com/community/">Neo4j Community</a></p>
    </div>

  </body>

</html>

