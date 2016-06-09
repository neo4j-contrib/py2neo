<!doctype html>
<html>

  <head>
    <title>{{person.name}} - The Movie Graph</title>
    <link rel="stylesheet" href="/css/main.css">
  </head>

  <body>

    <div class="header">
      <nav><a href="/">The Movie Graph</a> / <a href="/person/">People</a> / <strong>{{person.name}}</strong></nav>
    </div>

    <h1>{{person.name}}</h1>

    <h2>Personal Details</h2>
    <dl>
        <dt>Name:</dt>
          <dd>{{person.name}}</dd>
        <dt>Born:</dt>
          <dd>{{person.born}}</dd>
    </dl>

    <h2>Movies</h2>
    <ul>
    % for movie, role in sorted(movies):
        <li class="{{role}}"><a href="/movie/{{movie}}">{{movie}}</a> [{{role}}]</li>
    % end
    </ul>

    <div class="footer">
      <code>(graphs)-[:ARE]->(everywhere)</code>
      <p>With &hearts; from Sweden &amp; the <a href="http://neo4j.com/community/">Neo4j Community</a></p>
    </div>

  </body>

</html>

