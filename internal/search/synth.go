package search

import (
	"fmt"
	"math"
	"math/rand/v2"

	"github.com/mertmetin/listing-alerts/internal/event"
	"github.com/mertmetin/listing-alerts/internal/geo"
	"github.com/mertmetin/listing-alerts/internal/listing"
)

// Synthetic data for tests and benchmarks: saved searches and listing
// events spread over Ontario roughly like real demand (population-weighted
// cities), with the mix of shapes and filters real users pick.

type place struct {
	name   string
	center geo.Point
	weight float64
	radius float64 // km, rough extent of the city
}

var ontario = []place{
	{"Toronto", geo.Point{Lat: 43.6532, Lng: -79.3832}, 30, 18},
	{"Ottawa", geo.Point{Lat: 45.4215, Lng: -75.6972}, 11, 20},
	{"Mississauga", geo.Point{Lat: 43.5890, Lng: -79.6441}, 7, 12},
	{"Brampton", geo.Point{Lat: 43.7315, Lng: -79.7624}, 6, 10},
	{"Hamilton", geo.Point{Lat: 43.2557, Lng: -79.8711}, 5, 12},
	{"London", geo.Point{Lat: 42.9849, Lng: -81.2453}, 4, 10},
	{"Markham", geo.Point{Lat: 43.8561, Lng: -79.3370}, 3, 8},
	{"Vaughan", geo.Point{Lat: 43.8361, Lng: -79.4983}, 3, 9},
	{"Kitchener", geo.Point{Lat: 43.4516, Lng: -80.4925}, 2.5, 8},
	{"Windsor", geo.Point{Lat: 42.3149, Lng: -83.0364}, 2.2, 9},
	{"Oakville", geo.Point{Lat: 43.4675, Lng: -79.6877}, 2, 7},
	{"Burlington", geo.Point{Lat: 43.3255, Lng: -79.7990}, 1.8, 7},
	{"Oshawa", geo.Point{Lat: 43.8971, Lng: -78.8658}, 1.7, 7},
	{"Barrie", geo.Point{Lat: 44.3894, Lng: -79.6903}, 1.5, 7},
	{"Kingston", geo.Point{Lat: 44.2312, Lng: -76.4860}, 1.2, 6},
	{"Guelph", geo.Point{Lat: 43.5448, Lng: -80.2482}, 1.3, 6},
	{"Sudbury", geo.Point{Lat: 46.4917, Lng: -80.9930}, 1.1, 10},
	{"Waterloo", geo.Point{Lat: 43.4643, Lng: -80.5204}, 1.1, 5},
	{"St. Catharines", geo.Point{Lat: 43.1594, Lng: -79.2469}, 1.1, 6},
	{"Thunder Bay", geo.Point{Lat: 48.3809, Lng: -89.2477}, 0.8, 8},
}

// canada adds the other large Canadian markets to Ontario's, weighted by
// population, for Canada-wide load tests.
var canada = append(append([]place{}, ontario...), []place{
	{"Montreal", geo.Point{Lat: 45.5019, Lng: -73.5674}, 25, 18},
	{"Vancouver", geo.Point{Lat: 49.2827, Lng: -123.1207}, 17, 15},
	{"Calgary", geo.Point{Lat: 51.0447, Lng: -114.0719}, 10, 15},
	{"Edmonton", geo.Point{Lat: 53.5461, Lng: -113.4938}, 9, 14},
	{"Quebec City", geo.Point{Lat: 46.8139, Lng: -71.2080}, 5, 10},
	{"Winnipeg", geo.Point{Lat: 49.8951, Lng: -97.1384}, 5, 10},
	{"Halifax", geo.Point{Lat: 44.6488, Lng: -63.5752}, 3, 8},
	{"Saskatoon", geo.Point{Lat: 52.1332, Lng: -106.6700}, 2, 7},
	{"Regina", geo.Point{Lat: 50.4452, Lng: -104.6189}, 1.6, 6},
	{"Victoria", geo.Point{Lat: 48.4284, Lng: -123.3656}, 2.5, 6},
}...)

var subTypes = []string{"Detached", "Semi-Detached", "Att/Row/Townhouse", "Condo Apartment", "Condo Townhouse", "Duplex", "Link", "Lower Level"}

// Synth generates synthetic searches and events.
type Synth struct {
	rng    *rand.Rand
	total  float64
	places []place
}

// NewSynth seeds a generator over Ontario (same seed, same data).
func NewSynth(seed uint64) *Synth { return newSynth(seed, ontario) }

// NewSynthCanada seeds a generator over Canada's large markets.
func NewSynthCanada(seed uint64) *Synth { return newSynth(seed, canada) }

func newSynth(seed uint64, places []place) *Synth {
	s := &Synth{rng: rand.New(rand.NewPCG(seed, seed^0x9e3779b97f4a7c15)), places: places}
	for _, p := range places {
		s.total += p.weight
	}
	return s
}

// Rand exposes the generator's source (load generator lifecycles).
func (s *Synth) Rand() *rand.Rand { return s.rng }

// Place returns a population-weighted city name and a point in it.
func (s *Synth) Place() (string, geo.Point) {
	p := s.place()
	return p.name, s.near(p.center, p.radius*1.1)
}

// SubType returns a random property sub-type.
func (s *Synth) SubType() string { return subTypes[s.rng.IntN(len(subTypes))] }

func (s *Synth) place() place {
	x := s.rng.Float64() * s.total
	for _, p := range s.places {
		if x -= p.weight; x <= 0 {
			return p
		}
	}
	return s.places[0]
}

// near returns a point within km of c (uniform over the disc).
func (s *Synth) near(c geo.Point, km float64) geo.Point {
	r := km * math.Sqrt(s.rng.Float64())
	a := s.rng.Float64() * 2 * math.Pi
	return geo.Point{
		Lat: c.Lat + r/111.32*math.Sin(a),
		Lng: c.Lng + r/(111.32*math.Cos(c.Lat*math.Pi/180))*math.Cos(a),
	}
}

// blob is an irregular polygon around c with radius about km.
func (s *Synth) blob(c geo.Point, km float64) geo.Polygon {
	n := 8 + s.rng.IntN(13)
	pg := make(geo.Polygon, n)
	for i := range pg {
		a := 2 * math.Pi * float64(i) / float64(n)
		r := km * (0.6 + 0.8*s.rng.Float64())
		pg[i] = geo.Point{
			Lat: c.Lat + r/111.32*math.Sin(a),
			Lng: c.Lng + r/(111.32*math.Cos(c.Lat*math.Pi/180))*math.Cos(a),
		}
	}
	return pg
}

// Search returns a synthetic saved search. Mix: 75% a drawn neighbourhood
// (0.7-4 km), 15% a wider drawn area (5-25 km), 10% picked by city name.
// Most set a budget, many a minimum of bedrooms: real searches are narrow.
func (s *Synth) Search(i int) *Search {
	p := s.place()
	sr := &Search{
		ID: fmt.Sprintf("s%07d", i), Site: "mertmetin", UserID: fmt.Sprintf("u%06d", i/3),
		Email: fmt.Sprintf("u%06d@example.com", i/3), Audience: Public, Transaction: "sale",
	}
	if s.rng.IntN(3) == 0 {
		sr.Transaction = "lease"
	}
	if s.rng.IntN(4) == 0 {
		sr.Audience = VOW
	}
	switch x := s.rng.IntN(100); {
	case x < 75:
		sr.Areas = []geo.Polygon{s.blob(s.near(p.center, p.radius), 0.7+3.3*s.rng.Float64())}
	case x < 90:
		sr.Areas = []geo.Polygon{s.blob(s.near(p.center, p.radius), 5+20*s.rng.Float64())}
	default:
		sr.Cities = []string{NormCity(p.name)}
	}
	base := 400000.0
	if sr.Transaction == "lease" {
		base = 1800
	}
	if s.rng.IntN(10) < 9 {
		sr.MaxPrice = math.Round(base * (1 + 2*s.rng.Float64()))
		if s.rng.IntN(3) != 0 {
			sr.MinPrice = math.Round(sr.MaxPrice * 0.7)
		}
	}
	if s.rng.IntN(2) == 0 {
		sr.MinBeds = 1 + s.rng.IntN(4)
	}
	if s.rng.IntN(4) == 0 {
		sr.MinBaths = 1 + s.rng.IntN(2)
	}
	if s.rng.IntN(3) == 0 {
		sr.Types = []string{subTypes[s.rng.IntN(len(subTypes))], subTypes[s.rng.IntN(len(subTypes))]}
	}
	return sr
}

var synthTypes = []event.Type{event.NewListing, event.NewListing, event.NewListing, event.PriceDrop, event.PriceDrop, event.Sold, event.Leased, event.BackOnMarket, event.NowPublic, event.Pending}

// Event returns a synthetic listing event. 5% have no coordinates
// (geocoding failed) and fall back to city matching.
func (s *Synth) Event(i int) event.ListingEvent {
	p := s.place()
	pt := s.near(p.center, p.radius*1.2)
	lease := s.rng.IntN(3) == 0
	price := 300000 + 1500000*s.rng.Float64()*s.rng.Float64()
	tx := "For Sale"
	if lease {
		price, tx = 1200+3500*s.rng.Float64(), "For Lease"
	}
	beds, baths := 1+s.rng.IntN(5), 1+s.rng.IntN(3)
	l := listing.Listing{
		Key: fmt.Sprintf("X%08d", i), TransactionType: tx, City: p.name,
		PropertySubType: subTypes[s.rng.IntN(len(subTypes))], Bedrooms: &beds, Bathrooms: &baths,
	}
	if s.rng.IntN(20) != 0 {
		l.Latitude, l.Longitude = &pt.Lat, &pt.Lng
	}
	return event.ListingEvent{
		ID: fmt.Sprintf("e%08d", i), Type: synthTypes[s.rng.IntN(len(synthTypes))],
		ListingKey: l.Key, Price: &price, Public: s.rng.IntN(10) != 0, Listing: l,
	}
}
