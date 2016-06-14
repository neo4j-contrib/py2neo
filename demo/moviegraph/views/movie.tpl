<!doctype html>
<html>

  <head>
    <title>{{movie.title}} [{{movie.released}}] - The Movie Graph</title>
    <link rel="stylesheet" href="/css/main.css">
  </head>

  <body>

    <div class="header">
      <nav><a href="/">The Movie Graph</a> / <a href="/movies/">Movies</a> / <strong>{{movie.title}}</strong></nav>
    </div>

    <h1>{{movie.title}}</h1>

    <h2>Movie Details</h2>
    <dl>
        <dt>Title:</dt>
          <dd>{{movie.title}}</dd>
        <dt>Released:</dt>
          <dd>{{movie.released}}</dd>
        <dt>Directors:</dt>
          <dd>
            % for director in sorted(movie.directors):
                <a href="/person/{{director.name}}">{{director.name}}</a><br>
            % end
          </dd>
    </dl>

    <h2>Cast</h2>
    <ul>
    % for actor in sorted(movie.actors):
        <li><a href="/person/{{actor.name}}">{{actor.name}}</a></li>
    % end
    </ul>
    
    <h2>Comments</h2>
    % for comment in sorted(movie.comments, reverse=True):
        <p>On {{comment.date}}, {{comment.name}} said...
          <blockquote>{{comment.text}}</blockquote>
        </p>
    % end
    
    <form method="POST" action="comment">
      <h3>Submit a new comment</h3>

      <input type="hidden" name="title" value="{{movie.title}}">

      <label>Name:<br>
      <input type="text" name="name" value="">
      </label><br>

      <label>Comments:<br>
      <textarea name="text" cols="80" rows="6"></textarea>
      </label><br>

      <input type="submit" value="Submit">

    </form>

    <div class="footer">
      <code>(graphs)-[:ARE]->(everywhere)</code>
    </div>

  </body>

</html>

