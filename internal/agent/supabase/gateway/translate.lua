-- Upstream Supabase Envoy policy, consolidated around route names. RBAC runs
-- between the authorization and translation instances against RAW credentials.
local protected = { auth=true, rest=true, ["rest-root"]=true, graphql=true,
  ["realtime-api"]=true, ["realtime-ws"]=true, meta=true }
local function query_key(path)
  local q = string.find(path, "?", 1, true)
  if not q then return nil end
  for key, value in string.gmatch(string.sub(path, q+1), "([^&]+)=([^&]*)") do
    if key == "apikey" and value ~= "" then return value end
  end
end
local function reject(h, message)
  h:respond({[":status"]="401", ["content-type"]="text/plain"}, message)
end
function envoy_on_request(h)
  local headers = h:headers()
  local route = h:streamInfo():routeName()
  if route == "deny" then return end
  local path = headers:get(":path")
  local key = headers:get("apikey")
  if authorize_only then
    if route ~= "functions" and (key == nil) then
      key = query_key(path)
      if key then headers:replace("apikey", key) end
    end
    if protected[route] and not known[key] then reject(h, "Unauthorized") end
    return
  end
  headers:remove("sb-api-key")
  headers:remove("x-consumer-id")
  headers:remove("x-consumer-username")
  headers:remove("x-consumer-custom-id")
  headers:remove("x-authenticated-groups")
  headers:replace("x-forwarded-host", public_host)
  headers:replace("x-forwarded-port", public_port)
  headers:replace("x-forwarded-proto", public_scheme)
  if route == "studio" then return end
  local auth = headers:get("authorization")
  if route == "functions" then
    if next(mapped) == nil then return end
    local bearer = auth and string.match(auth, "^[Bb]earer%s+(.+)$")
    if bearer and string.sub(bearer,1,3) == "sb_" then
      if key and key ~= "" and key ~= bearer then reject(h,"Conflicting API keys");return end
      if not key or key == "" then key = bearer end
    end
    if key and string.sub(key,1,3) == "sb_" then
      if not mapped[key] then reject(h,"Invalid API key");return end
      headers:replace("sb-api-key",mapped[key])
    end
    return
  end
  -- Header precedence is authoritative. Rewrite only the matching query key;
  -- never let a lower-priority query credential elevate an authenticated header.
  local translated = key and mapped[key]
  if translated then
    headers:replace("apikey",translated)
    local q = string.find(path,"?",1,true)
    if q then
      local query = string.sub(path,q+1)
      query = string.gsub(query,"([^&]+)",function(part)
        if part == "apikey="..key then return "apikey="..translated end
        return part
      end)
      headers:replace(":path",string.sub(path,1,q)..query)
    end
  end
  local downstream_key = translated or key
  if route == "realtime-ws" then
    if downstream_key then headers:replace("x-api-key",downstream_key) end
    return
  end
  -- Storage authenticates S3 itself. Preserve nonempty Authorization, including
  -- SigV4, user JWTs and other schemes; remove whitespace for presigned requests.
  if auth and string.match(auth,"^%s*$") then headers:remove("authorization");auth=nil end
  if auth and string.sub(auth,1,10) ~= "Bearer sb_" then return end
  if downstream_key and downstream_key ~= "" then
    headers:replace("authorization","Bearer "..downstream_key)
  end
end
