package selector

import (
	"errors"
	"io/ioutil"

	"github.com/DCSO/balboa/observation"
	lua "github.com/yuin/gopher-lua"
)

const (
	luaInputObservationTypeName = "observation"
	luaProcessObservationFunc   = "process"
)

func init() {
	err := RegisterSelector("lua", MakeLuaSelector)
	if err != nil {
		panic(err)
	}
}

type LuaSelector struct {
	luaScript string
	ingest    []string
	L         *lua.LState
}

func MakeLuaSelector(config interface{}) (selector Selector, err error) {
	l := &LuaSelector{}
	if configKV, ok := config.(map[interface{}]interface{}); ok {
		if scriptOption, ok := configKV["script"]; ok {
			scriptFile := scriptOption.(string)
			scriptContent, err := ioutil.ReadFile(scriptFile)
			if err != nil {
				return nil, err
			}
			l.luaScript = string(scriptContent)
		} else {
			return nil, errors.New("type assertion failed")
		}
		if ingestListInterface, ok := configKV["ingest"]; ok {
			if ingestList, ok := ingestListInterface.([]interface{}); ok {
				for _, tag := range ingestList {
					l.ingest = append(l.ingest, tag.(string))
				}
			} else {
				return nil, errors.New("type assertion failed")
			}
		}
	} else {
		return nil, errors.New("type assertion failed")
	}
	if err = l.InitializeLua(); err != nil {
		return nil, err
	}
	return l, nil
}

func checkObservation(L *lua.LState) *observation.InputObservation {
	o := L.CheckUserData(1)
	if v, ok := o.Value.(*observation.InputObservation); ok {
		return v
	}
	L.ArgError(1, "observation expected")
	return nil
}

func observationGetRcode(L *lua.LState) int {
	o := checkObservation(L)
	L.Push(lua.LString(o.Rcode))
	return 1
}

func observationGetRdata(L *lua.LState) int {
	o := checkObservation(L)
	L.Push(lua.LString(o.Rdata))
	return 1
}

func observationGetRrtype(L *lua.LState) int {
	o := checkObservation(L)
	L.Push(lua.LString(o.Rrtype))
	return 1
}

func observationGetRrname(L *lua.LState) int {
	o := checkObservation(L)
	L.Push(lua.LString(o.Rrname))
	return 1
}

func observationGetSensorId(L *lua.LState) int {
	o := checkObservation(L)
	L.Push(lua.LString(o.SensorID))
	return 1
}

func observationGetTags(L *lua.LState) int {
	o := checkObservation(L)
	t := L.NewTable()
	for tag, _ := range o.Tags {
		t.Append(lua.LString(tag))
	}
	L.Push(t)
	return 1
}

func observationAddTag(L *lua.LState) int {
	o := checkObservation(L)
	tag := L.ToString(2)
	if tag != "" {
		o.Tags[tag] = struct{}{}
	}
	return 0
}

var observationMethods = map[string]lua.LGFunction{
	"rcode":     observationGetRcode,
	"rdata":     observationGetRdata,
	"rrtype":    observationGetRrtype,
	"rrname":    observationGetRrname,
	"sensor_id": observationGetSensorId,
	"tags":      observationGetTags,
	"add_tag":   observationAddTag,
}

func (l *LuaSelector) registerInputObservation() {
	mt := l.L.NewTypeMetatable(luaInputObservationTypeName)
	l.L.SetGlobal(luaInputObservationTypeName, mt)
	l.L.SetField(mt, "__index", l.L.SetFuncs(l.L.NewTable(), observationMethods))
}

func (l *LuaSelector) InitializeLua() (err error) {
	l.L = lua.NewState()
	l.registerInputObservation()
	err = l.L.DoString(l.luaScript)
	return err
}

func (l *LuaSelector) Reinitialize() (err error) {
	panic("implement me")
}

func (l *LuaSelector) ProcessObservation(observation *observation.InputObservation) (err error) {
	fn := l.L.GetGlobal(luaProcessObservationFunc)
	if err := l.L.CallByParam(lua.P{
		Fn:      fn,
		NRet:    0,
		Protect: true,
	}, &lua.LUserData{
		Value:     observation,
		Metatable: l.L.GetTypeMetatable(luaInputObservationTypeName),
	}); err != nil {
		return err
	}
	return err
}

func (l *LuaSelector) IngestionList() []string {
	return l.ingest
}
