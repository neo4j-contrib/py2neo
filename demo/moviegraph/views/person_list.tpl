<!doctype html>
<html>

  <head>
    <title>Person List - The Movie Graph</title>
    <link rel="stylesheet" href="/css/main.css">
  </head>

  <body>

    <div class="header">
      <nav><a href="/">The Movie Graph</a> / <strong>People</strong></nav>
    </div>

    <h1>People</h1>
    <ul>
    % for person in people:
        <li><a href="/person/{{person.name}}">{{person.name}}</a></li>
    % end
    </ul>

    <div class="footer">
      <code>(graphs)-[:ARE]->(everywhere)</code>
      <p>With &hearts; from Sweden &amp; the <a href="http://neo4j.com/community/">Neo4j Community</a></p>
    </div>

  </body>

</html>

