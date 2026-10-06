 -- This program will register a menu that will open a window with a count of occurrences
-- of every address in the capture

local function menuable_tap()
	-- Declare the window we will use
	local tw = TextWindow.new("Address Counter")

	-- This will contain a hash of counters of appearances of a certain address
	local src_ips = {}
	local src_lens = {}
	local dst_ips = {}
	local dst_lens = {}

	-- this is our tap
	local tap = Listener.new("tcp");

	local function remove()
		-- this way we remove the listener that otherwise will remain running indefinitely
		tap:remove();
	end

	-- we tell the window to call the remove() function when closed
	tw:set_atclose(remove)

	-- this function will be called once for each packet
	function tap.packet(pinfo, tvb)
        	if pinfo.dst_port == 1715 then 
	        	src_ips[tostring(pinfo.src)]  = (src_ips[tostring(pinfo.src)]  or 0) + 1
	        	src_lens[tostring(pinfo.src)] = (src_lens[tostring(pinfo.src)] or 0) + pinfo.caplen
        	end
	        if pinfo.src_port == 1715 then 
    			dst_ips[tostring(pinfo.dst)]  = (dst_ips[tostring(pinfo.dst)]  or 0) + 1
	    		dst_lens[tostring(pinfo.dst)] = (dst_lens[tostring(pinfo.dst)] or 0) + pinfo.caplen
        	end
	end

	-- this function will be called once every few seconds to update our window
	function tap.draw(t)
		tw:clear() 
		tw:append("\nsource: \t\ttimes \tlength \n");
		for ip, num in pairs(src_ips) do
			tw:append(ip .. "\t" .. num .. "\t" .. src_lens[ip] .. "\n");
		end
		tw:append("\ndest: \t\ttimes \tlength \n");
		for ip, num in pairs(dst_ips) do
			tw:append(ip .. "\t" .. num .. "\t" .. dst_lens[ip] .. "\n");
		end
	end

	-- this function will be called whenever a reset is needed
	-- e.g. when reloading the capture file
	function tap.reset()
		tw:clear()
		src_ips = {}
		src_lens = {}
		dst_ips = {}
		dst_lens = {}
	end

	-- Ensure that all existing packets are processed.
	retap_packets()
end

-- using this function we register our function
-- to be called when the user selects the Tools->Test->Packets menu
register_menu("Test/Packets", menuable_tap, MENU_TOOLS_UNSORTED)