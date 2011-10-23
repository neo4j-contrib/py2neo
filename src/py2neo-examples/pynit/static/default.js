$(window).load(function() {

	/*
	 * Page reload function
	 */
	function reload() {
		location.reload(true);
	}

	/*
	 * Function to generate a colour from a bookmark handle
	 */
	function convertHandleToColour(handle) {
		var handleChars = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz",
		    hues = [
		    	0, 5, 10, 15, 20, 25,
		    	30, 35, 40, 45, 50, 55,
		    	60, 70, 80, 90, 100, 110,
		    	120, 140, 160, 180, 200, 220,
		    	240, 250, 260, 270, 280, 290,
		    	300, 310, 320, 330, 340, 350
		    ],
		    hue = 0;
		for(var i = 0; i < handle.length; i++) {
			hue ^= handleChars.indexOf(handle.charAt(i));
		}
		return "hsl(" + hues[Math.floor(hues.length * hue / handleChars.length)] + ",60%,40%)";
	}

	/*
	 * Add the logo
	 */
	$("body").prepend('<img id="logo" src="/static/logo.png" />');

	/*
	 * Determine minimum and maximum number of hits across bookmarks
	 */
	var minHits = null,
	    maxHits = null;
	$(".bookmark .hits").each(function() {
		var hits = parseInt($(this).text());
		minHits = minHits == null || hits < minHits ? hits : minHits;
		maxHits = maxHits == null || hits > maxHits ? hits : maxHits;
	});

	/*
	 * Determine minimum and maximum number of hits across bookmarks
	 */
	$(".bookmark").each(function() {
		var handle = $(".handle", this).text(),
		    hits   = parseInt($(".hits", this).text());
		$(this).css("font-size", 10 + (15 * (hits - minHits)) / (maxHits - minHits))
		       .css("background-color", convertHandleToColour(handle))
		       .append('<span class="delete action">&#x2715;</span>');
	});

	/*
	 * Bind "add bookmark" event handler
	 */
	$("#add_bookmark").click(function() {
		$.ajax({
			type: "POST",
			url: "/",
			data: JSON.stringify({
				"address": $("#new_address").val()
			}),
			success: reload,
			contentType: "application/json",
			dataType: "json"
		});
	})

	/*
	 * Bind "delete bookmark" event handlers
	 */
	$(".bookmark .delete.action").click(function() {
		var handle = $(this).siblings(".handle").text();
		if(confirm("Please confirm deletion of bookmark " + handle)) {
			$.ajax({
				type: "DELETE",
				url: "/" + handle,
				success: reload,
				contentType: "application/json",
				dataType: "json"
			});
		}
	});

});

