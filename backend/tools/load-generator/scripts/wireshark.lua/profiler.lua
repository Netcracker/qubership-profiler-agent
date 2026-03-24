profiler_protocol = Proto("CDT2", "CDT Profiler Protocol")

-- Header fields

local commands = { 
   [0x01] = "COMMAND_INIT_STREAM", 
   [0x15] = "COMMAND_INIT_STREAM_V2",
   [0x02] = "COMMAND_RCV_DATA",
   [0x04] = "COMMAND_CLOSE",
   [0x08] = "COMMAND_GET_PROTOCOL_VERSION",
   [0x14] = "COMMAND_GET_PROTOCOL_VERSION_V2",
   [0x11] = "COMMAND_REQUEST_ACK_FLUSH",
   [0x13] = "COMMAND_REPORT_COMMAND_RESULT" 
}
command_id = ProtoField.uint8("cdt.command_id", "Command Id", base.HEX, commands)

-- COMMAND_GET_PROTOCOL_VERSION_V2
protocol_ver = ProtoField.int64("cdt.protocol_ver", "protocol version", base.DEC)
pod_name     = ProtoField.string("cdt.pod", "pod name", base.ASCII)
microservice = ProtoField.string("cdt.microservice", "microservice", base.ASCII)
namespace    = ProtoField.string("cdt.namespace", "namespace", base.ASCII)

-- COMMAND_INIT_STREAM_V2
stream_name       = ProtoField.string("cdt.stream_name", "streamName", base.ASCII)
requested_seq_id  = ProtoField.uint32("cdt.requested_seq_id", "requested rolling sequence id", base.DEC)
reset_required    = ProtoField.uint32("cdt.reset_required", "reset required", base.DEC)

rotation_period = ProtoField.int64("cdt.rotation_period", "rotation period", base.DEC)
rotation_size   = ProtoField.int64("cdt.rotation_size", "required rotation size", base.DEC)
rolling_seq_id  = ProtoField.uint32("cdt.rolling_seq_id", "rolling sequence id", base.DEC)

-- COMMAND_RCV_DATA
handle_id      = ProtoField.guid("cdt.handle_id", "handle_id")
content_len    = ProtoField.uint32("cdt.content_len", "length", base.DEC)
content_data   = ProtoField.bytes("cdt.content_data", "data", base.DASH)

-- COMMAND_REQUEST_ACK_FLUSH
pod_command_id = ProtoField.guid("cdt.pod_command_id", "command id")
pod_comman_res = ProtoField.uint8("cdt.pod_comman_res", "command result", base.DEC)

-- columns
c_streams      = ProtoField.string("cdt.streams", "handles", base.ASCII)
c_commands     = ProtoField.string("cdt.commands", "handles", base.ASCII)
c_handles      = ProtoField.string("cdt.handles", "handles", base.ASCII)

-- system
warning = ProtoField.string("cdt.warning", "warning", base.ASCII)
request = ProtoField.string("cdt.request", "previous request command", base.ASCII)
other = ProtoField.bytes("cdt.other", "other", base.SPACE)
merged = ProtoField.bytes("cdt.merged", "merged data", base.SPACE)
missed = ProtoField.bytes("cdt.missed", "missed from previous packet", base.SPACE)

profiler_protocol.fields = {
  command_id, 
  protocol_ver, pod_name, microservice, namespace,
  stream_name, requested_seq_id, reset_required,
  rotation_period, rotation_size, rolling_seq_id,
  handle_id, content_len, content_data,
  pod_command_id, pod_comman_res,
  c_streams, c_commands, c_handles,
  warning, request, missed, merged, other 
}

merged_note = ProtoExpert.new("cdt.merged.note", "merged data", expert.group.COMMENTS_GROUP, expert.severity.WARN)

profiler_protocol.experts = {
  merged_note
}


prev_state = ""
prev_init_stream = ""
streams = {}

function keys(tab)  
  if tab == nil or next(tab) == nil then
    return ""
  end

  local ctab = {}
  local n = 1
  for k, v in pairs(tab) do
      ctab[n] = k
      n = n + 1
  end
  return table.concat(ctab, ", ")
end

function profiler_protocol.dissector(buffer, pinfo, tree)
    length = buffer:len()
    if length == 0 then 
      return 
    end

    print("start parsing")

    pinfo.conversation = profiler_protocol
    pinfo.cols.protocol = profiler_protocol.name
    local subtree = tree:add(profiler_protocol, buffer(), "CDT Profiler Protocol Data")

    print( "packet", length  )
    print( "prev", prev_state, prev_init_stream )
    print( "ports", pinfo.dst_port, pinfo.src_port )

    local set_handles = {}
    local set_commands = {}
    local set_streams = {}

    if pinfo.dst_port == 1715 then  -- request
      print( "request", pinfo.dst_port )
      local pos = 0
      prev_init_stream = ""

      local data = {} -- chunks from COMMAND_RCV_DATA

      while (pos >= 0) and (pos < length) do
        subtree:add_le(command_id, buffer(pos,1))
        local command_name = commands[buffer(pos,1):le_uint()]
        print( command_id, command_name )
        pos = pos + 1
        
        if command_name == nil then 
          goto continue 
        end

        set_commands[command_name] = true
        if command_name == "COMMAND_INIT_STREAM" then
          pos = -1
        elseif command_name == "COMMAND_INIT_STREAM_V2" then
          pos, slen, buf = readString(buffer, pos)
          subtree:add(stream_name, buf)
          prev_init_stream = buf:string()
          set_streams[prev_init_stream] = true

          subtree:add(requested_seq_id, buffer(pos, 4))
          pos = pos + 4
          subtree:add(reset_required, buffer(pos, 4))
          pos = pos + 4
        elseif command_name == "COMMAND_RCV_DATA" then
          if pos + 16 + 4 >= length then -- should append from next packet
            pinfo.desegment_offset = 0
            pinfo.desegment_len = DESEGMENT_ONE_MORE_SEGMENT
            return 
          end
          local handle = buffer(pos, 16):bytes():tohex()
          local meta = streams[handle]
          if meta == nil then
            meta = {stream = "unknown"}
          end
          subtree:add(handle_id, buffer(pos, 16)):append_text(" (" .. meta.stream .. ")")
          pos = pos + 16

          subtree:add(stream_name):append_text(meta.stream)
          set_streams[meta.stream] = true
          set_handles[handle] = true

          local slen = buffer(pos, 4):uint()
          subtree:add(content_len, buffer(pos, 4))
          pos = pos + 4
          local alen = math.max(0, math.min(slen, length-pos))
          subtree:add(content_data, buffer(pos, alen)):append_text(" (" .. alen .. ")")
          local chunk = buffer(pos, alen):bytes():tohex()
          pos = pos + alen

          if alen < slen then -- should append from next packet
            pinfo.desegment_offset = 0
            pinfo.desegment_len = DESEGMENT_ONE_MORE_SEGMENT
            return 
          end
          table.insert(data, chunk)

        elseif command_name == "COMMAND_CLOSE" then
          pos = -1
        elseif command_name == "COMMAND_GET_PROTOCOL_VERSION" then
          pos = -1
        elseif command_name == "COMMAND_GET_PROTOCOL_VERSION_V2" then
          subtree:add(protocol_ver, buffer(pos, 8))
          pos = pos + 8
          pos, slen, buf = readString(buffer, pos)
          subtree:add(pod_name, buf)
          pos, slen, buf = readString(buffer, pos)
          subtree:add(microservice, buf)
          pos, slen, buf = readString(buffer, pos)
          subtree:add(namespace, buf)
        elseif command_name == "COMMAND_REQUEST_ACK_FLUSH" then
          pos = -1
        elseif command_name == "COMMAND_REPORT_COMMAND_RESULT" then
          subtree:add(pod_command_id, buffer(pos, 16))
          pos = pos + 16
          subtree:add(pod_comman_res, buffer(pos, 1))
          pos = pos + 1
        end -- if
        prev_state = command_name

        print( "finish request", pos, prev_state )

        ::continue::
      end -- while

      if next(data) ~= nil then
          -- have (several?) COMMAND_RCV_DATA and received full data
          local s = table.concat(data, "")
          -- subtree:add_proto_expert_info(merged_note, " [" .. s:len() .. "](" .. s .. ")")
          subtree:add(merged):set_text(" ----------- ")
          subtree:add(merged):set_text(" [" .. s:len() .. "](" .. s .. ")")
          pos = length
      end

      if (pos == -1) and (length > 0) then
        print( "finish other", pos, length )
        subtree:add(other, buffer(0, length)):append_text(" (" .. length .. ")")
      end
    elseif pinfo.src_port == 1715 then  
      -- response
        subtree:add(warning):set_text(" (please click to response AFTER clicking to request)")
        subtree:add(request):set_text(" (" .. prev_state .. ")")
        print( "response", pinfo.src_port )

        local pos = 0

        if prev_state == "COMMAND_INIT_STREAM" then
          pos = length + 1
        elseif prev_state == "COMMAND_INIT_STREAM_V2" then
          subtree:add(handle_id, buffer(pos, 16))
          local handle =  buffer(pos, 16):bytes():tohex()
          pos = pos + 16
          subtree:add(rotation_period, buffer(pos, 8))
          pos = pos + 8
          subtree:add(rotation_size, buffer(pos, 8))
          pos = pos + 8
          subtree:add(rolling_seq_id, buffer(pos, 4))
          pos = pos + 4

          streams[handle] = {id=handle, stream=prev_init_stream}
          set_handles[handle] = true
          set_streams[prev_init_stream] = true
        elseif prev_state == "COMMAND_RCV_DATA" then
          pos = length + 1
        elseif prev_state == "COMMAND_CLOSE" then
          pos = length + 1
        elseif prev_state == "COMMAND_GET_PROTOCOL_VERSION" then
          subtree:add(protocol_ver, buffer(pos, 8))
          pos = pos + 8
        elseif prev_state == "COMMAND_GET_PROTOCOL_VERSION_V2" then
          subtree:add(protocol_ver, buffer(pos, 8))
          pos = pos + 8
        elseif prev_state == "COMMAND_REQUEST_ACK_FLUSH" then
          pos = length + 1
        elseif prev_state == "COMMAND_REPORT_COMMAND_RESULT" then
          pos = length + 1
        end -- if

        print( "finish response", pos, prev_state )

        if pos > length then
          subtree:add(other, buffer(0, length)):set_text(" (" .. length .. ")")
        end
    else
      print( "unknown", pinfo )
    end -- response


    pinfo.cols.info:set(" ".. pinfo.src_port .. "  → " .. pinfo.dst_port .."  ")
    if next(set_commands) ~= nil then
      pinfo.cols.info:append(" ".. keys(set_commands) .." ")
    end
    if next(set_streams) ~= nil then
      pinfo.cols.info:append(" ( ".. keys(set_streams) .." )")
    end
    if next(set_handles) ~= nil then
      pinfo.cols.info:append(" ( ".. keys(set_handles) .." )")
    end

    print( "continue?", pinfo.desegment_len, pinfo.desegment_offset )

end

readString = function (buffer, old_pos)
  local pos = old_pos  
  local slen = buffer(pos, 4):uint()
  local pos = pos + 4
  local buff = buffer(pos, slen)
  local name = buff:string()
  pos = pos + slen
  -- print(old_pos, pos, slen, name)
  return pos, slen, buff
end


local tcp_port = DissectorTable.get("tcp.port")
tcp_port:add(1715, profiler_protocol)
