import { useState, useEffect, useMemo } from 'react';
import { Search, BedDouble, Bath, MapPin, ExternalLink, Home, Building, ChevronDown, ThumbsUp, ThumbsDown } from 'lucide-react';
import { Popover } from '@headlessui/react';
import { motion, AnimatePresence } from 'framer-motion';

const bedOptions = [
  { label: 'Any', value: '' },
  { label: 'Studio', value: '0' },
  { label: '1', value: '1' },
  { label: '2', value: '2' },
  { label: '3', value: '3' },
  { label: '4', value: '4' },
  { label: '5+', value: '5' },
];

const bathOptions = [
  { label: 'Any', value: '' },
  { label: '1+', value: '1' },
  { label: '1.5+', value: '1.5' },
  { label: '2+', value: '2' },
  { label: '2.5+', value: '2.5' },
  { label: '3+', value: '3' },
  { label: '4+', value: '4' },
];

const typeOptions = [
  { label: 'House', value: 'house', icon: Home },
  { label: 'Apartment', value: 'apartment', icon: Building },
];

const platformOptions = [
  { label: 'Zillow', value: 'zillow' },
  { label: 'Redfin', value: 'redfin' },
];

function App() {
  const [rentals, setRentals] = useState([]);
  const [loading, setLoading] = useState(false);

  // Filters
  const [minPrice, setMinPrice] = useState('');
  const [maxPrice, setMaxPrice] = useState('');
  const [minBeds, setMinBeds] = useState('');
  const [minBaths, setMinBaths] = useState('');
  const [minSqft, setMinSqft] = useState('');
  const [maxSqft, setMaxSqft] = useState('');
  const [city, setCity] = useState('');
  const [zip, setZip] = useState('');
  const [propertyType, setPropertyType] = useState([]);
  const [selectedPlatforms, setSelectedPlatforms] = useState([]);

  // Like / Dislike State
  const [likedIds, setLikedIds] = useState(() => {
    const saved = localStorage.getItem('likedRentals');
    return saved ? JSON.parse(saved) : [];
  });
  const [dislikedIds, setDislikedIds] = useState(() => {
    const saved = localStorage.getItem('dislikedRentals');
    return saved ? JSON.parse(saved) : [];
  });

  useEffect(() => {
    localStorage.setItem('likedRentals', JSON.stringify(likedIds));
  }, [likedIds]);

  useEffect(() => {
    localStorage.setItem('dislikedRentals', JSON.stringify(dislikedIds));
  }, [dislikedIds]);

  const toggleLike = (id) => {
    if (likedIds.includes(id)) {
      setLikedIds(prev => prev.filter(i => i !== id));
    } else {
      setLikedIds(prev => [...prev, id]);
      setDislikedIds(prev => prev.filter(i => i !== id));
    }
  };

  const toggleDislike = (id) => {
    if (dislikedIds.includes(id)) {
      setDislikedIds(prev => prev.filter(i => i !== id));
    } else {
      setDislikedIds(prev => [...prev, id]);
      setLikedIds(prev => prev.filter(i => i !== id));
    }
  };

  const sortedRentals = useMemo(() => {
    return [...rentals].sort((a, b) => {
      const aLiked = likedIds.includes(a.id);
      const bLiked = likedIds.includes(b.id);
      const aDisliked = dislikedIds.includes(a.id);
      const bDisliked = dislikedIds.includes(b.id);

      if (aLiked && !bLiked) return -1;
      if (!aLiked && bLiked) return 1;
      if (aDisliked && !bDisliked) return 1;
      if (!aDisliked && bDisliked) return -1;
      return 0;
    });
  }, [rentals, likedIds, dislikedIds]);

  const fetchRentals = async () => {
    setLoading(true);
    try {
      const params = new URLSearchParams();
      if (minPrice) params.append('min_price', minPrice);
      if (maxPrice) params.append('max_price', maxPrice);
      if (minBeds) params.append('min_beds', minBeds);
      if (minBaths) params.append('min_baths', minBaths);
      if (minSqft) params.append('min_sqft', minSqft);
      if (maxSqft) params.append('max_sqft', maxSqft);
      if (city) params.append('city', city);
      if (zip) params.append('zip', zip);
      if (propertyType && propertyType.length > 0) {
        propertyType.forEach(pt => params.append('property_type', pt));
      }
      if (selectedPlatforms && selectedPlatforms.length > 0) {
        selectedPlatforms.forEach(p => params.append('source', p));
      }

      const baseUrl = import.meta.env.DEV ? 'http://localhost:8123' : '';
      const response = await fetch(`${baseUrl}/api/rentals?${params.toString()}`);
      if (response.ok) {
        const data = await response.json();
        if (Array.isArray(data)) {
          setRentals(data);
        } else {
          console.error('API returned non-array data:', data);
          setRentals([]);
        }
      } else {
        console.error('Failed to fetch rentals');
        setRentals([]);
      }
    } catch (err) {
      console.error(err);
      setRentals([]);
    }
    setLoading(false);
  };

  useEffect(() => {
    fetchRentals();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  const handleSearch = (e) => {
    e.preventDefault();
    fetchRentals();
  };

  const handleReset = () => {
    setMinPrice('');
    setMaxPrice('');
    setMinBeds('');
    setMinBaths('');
    setMinSqft('');
    setMaxSqft('');
    setCity('');
    setZip('');
    setPropertyType([]);
    setSelectedPlatforms([]);
  };

  return (
    <div className="min-h-screen bg-[#070b14] font-sans relative pb-16">
      <div className="max-w-[1400px] mx-auto px-6 pt-12">
        <header className="mb-10 text-center">
          <h1 className="text-4xl md:text-5xl font-extrabold text-[#f8fafc] tracking-tight mb-2">
            IntelRentals
          </h1>
          <p className="text-slate-500">Your curated feed of high-quality properties.</p>
        </header>

        {/* Filter Form matches the dark form in screenshot */}
        <form onSubmit={handleSearch} className="bg-[#10141e] border border-[#1d2335] rounded-2xl p-6 lg:p-8 mb-10 shadow-xl">
          <div className="flex flex-col gap-8">

            {/* Filter Buttons Navigation */}
            <div className="flex flex-wrap items-center gap-3">
              {/* Platforms Popover */}
              <Popover className="relative">
                {({ open }) => (
                  <>
                    <Popover.Button className={`flex items-center gap-2 px-5 py-2.5 rounded-xl border font-bold text-sm transition-all focus:outline-none ${selectedPlatforms.length > 0 || open
                      ? 'bg-[#09292a] border-teal-600/50 text-teal-400'
                      : 'bg-[#0b0e17] border-[#1d2335] text-slate-200 hover:border-slate-500 hover:bg-[#121622]'
                      }`}>
                      Platforms
                      <ChevronDown size={14} className={`transition-transform duration-200 ${open ? 'rotate-180' : ''}`} />
                    </Popover.Button>
                    <Popover.Panel transition className="absolute z-50 mt-2 w-64 bg-[#10141e] border border-[#1d2335] rounded-xl shadow-2xl p-6 transition duration-200 ease-out data-[closed]:-translate-y-1 data-[closed]:opacity-0 data-[closed]:scale-95">
                      <div className="flex flex-col gap-4">
                        <label className="text-[13px] font-bold text-slate-200">Select Sources</label>
                        <div className="flex flex-col gap-2">
                          {platformOptions.map((opt) => (
                            <label key={opt.value} className="flex items-center gap-3 cursor-pointer group p-2 hover:bg-[#161b26] rounded-lg transition-colors">
                              <input
                                type="checkbox"
                                checked={selectedPlatforms.includes(opt.value)}
                                onChange={() => {
                                  if (selectedPlatforms.includes(opt.value)) {
                                    setSelectedPlatforms(selectedPlatforms.filter(p => p !== opt.value));
                                  } else {
                                    setSelectedPlatforms([...selectedPlatforms, opt.value]);
                                  }
                                }}
                                className="w-4 h-4 rounded border-[#1d2335] text-teal-500 focus:ring-teal-500 bg-[#0b0e17]"
                              />
                              <span className="text-sm font-medium text-slate-300 group-hover:text-white transition-colors">{opt.label}</span>
                            </label>
                          ))}
                        </div>
                        <div className="flex justify-end gap-3 mt-2 border-t border-[#1d2335] pt-4">
                          <button
                            type="button"
                            onClick={() => setSelectedPlatforms([])}
                            className="text-sm font-bold text-teal-500 hover:text-teal-400"
                          >
                            Reset
                          </button>
                          <Popover.Button className="px-6 py-2 bg-[#e93d56] hover:bg-[#d4354c] text-white rounded-lg transition-all font-bold text-sm">
                            Done
                          </Popover.Button>
                        </div>
                      </div>
                    </Popover.Panel>
                  </>
                )}
              </Popover>

              {/* Price Popover */}
              <Popover className="relative">
                {({ open }) => (
                  <>
                    <Popover.Button className={`flex items-center gap-2 px-5 py-2.5 rounded-xl border font-bold text-sm transition-all focus:outline-none ${minPrice || maxPrice || open
                      ? 'bg-[#09292a] border-teal-600/50 text-teal-400'
                      : 'bg-[#0b0e17] border-[#1d2335] text-slate-200 hover:border-slate-500 hover:bg-[#121622]'
                      }`}>
                      Price
                      <ChevronDown size={14} className={`transition-transform duration-200 ${open ? 'rotate-180' : ''}`} />
                    </Popover.Button>
                    <Popover.Panel transition className="absolute z-50 mt-2 w-80 bg-[#10141e] border border-[#1d2335] rounded-xl shadow-2xl p-6 transition duration-200 ease-out data-[closed]:-translate-y-1 data-[closed]:opacity-0 data-[closed]:scale-95">
                      <div className="flex flex-col gap-4">
                        <label className="text-[13px] font-bold text-slate-200">Price Range</label>
                        <div className="flex items-center gap-3">
                          <input
                            type="number"
                            value={minPrice}
                            onChange={e => setMinPrice(e.target.value)}
                            placeholder="Min"
                            className="w-full bg-[#0b0e17] border border-[#1d2335] text-slate-200 rounded-lg px-4 py-2.5 text-sm focus:outline-none focus:border-teal-500/50"
                          />
                          <span className="text-slate-500">-</span>
                          <input
                            type="number"
                            value={maxPrice}
                            onChange={e => setMaxPrice(e.target.value)}
                            placeholder="Max"
                            className="w-full bg-[#0b0e17] border border-[#1d2335] text-slate-200 rounded-lg px-4 py-2.5 text-sm focus:outline-none focus:border-teal-500/50"
                          />
                        </div>
                        <div className="flex justify-end gap-3 mt-2 border-t border-[#1d2335] pt-4">
                          <button
                            type="button"
                            onClick={() => { setMinPrice(''); setMaxPrice(''); }}
                            className="text-sm font-bold text-teal-500 hover:text-teal-400"
                          >
                            Reset
                          </button>
                          <Popover.Button className="px-6 py-2 bg-[#e93d56] hover:bg-[#d4354c] text-white rounded-lg transition-all font-bold text-sm">
                            Done
                          </Popover.Button>
                        </div>
                      </div>
                    </Popover.Panel>
                  </>
                )}
              </Popover>

              {/* Sqft Popover */}
              <Popover className="relative">
                {({ open }) => (
                  <>
                    <Popover.Button className={`flex items-center gap-2 px-5 py-2.5 rounded-xl border font-bold text-sm transition-all focus:outline-none ${minSqft || maxSqft || open
                      ? 'bg-[#09292a] border-teal-600/50 text-teal-400'
                      : 'bg-[#0b0e17] border-[#1d2335] text-slate-200 hover:border-slate-500 hover:bg-[#121622]'
                      }`}>
                      Sqft
                      <ChevronDown size={14} className={`transition-transform duration-200 ${open ? 'rotate-180' : ''}`} />
                    </Popover.Button>
                    <Popover.Panel transition className="absolute z-50 mt-2 w-80 bg-[#10141e] border border-[#1d2335] rounded-xl shadow-2xl p-6 transition duration-200 ease-out data-[closed]:-translate-y-1 data-[closed]:opacity-0 data-[closed]:scale-95">
                      <div className="flex flex-col gap-4">
                        <label className="text-[13px] font-bold text-slate-200">Square Feet</label>
                        <div className="flex items-center gap-3">
                          <input
                            type="number"
                            value={minSqft}
                            onChange={e => setMinSqft(e.target.value)}
                            placeholder="Min"
                            className="w-full bg-[#0b0e17] border border-[#1d2335] text-slate-200 rounded-lg px-4 py-2.5 text-sm focus:outline-none focus:border-teal-500/50"
                          />
                          <span className="text-slate-500">-</span>
                          <input
                            type="number"
                            value={maxSqft}
                            onChange={e => setMaxSqft(e.target.value)}
                            placeholder="Max"
                            className="w-full bg-[#0b0e17] border border-[#1d2335] text-slate-200 rounded-lg px-4 py-2.5 text-sm focus:outline-none focus:border-teal-500/50"
                          />
                        </div>
                        <div className="flex justify-end gap-3 mt-2 border-t border-[#1d2335] pt-4">
                          <button
                            type="button"
                            onClick={() => { setMinSqft(''); setMaxSqft(''); }}
                            className="text-sm font-bold text-teal-500 hover:text-teal-400"
                          >
                            Reset
                          </button>
                          <Popover.Button className="px-6 py-2 bg-[#e93d56] hover:bg-[#d4354c] text-white rounded-lg transition-all font-bold text-sm">
                            Done
                          </Popover.Button>
                        </div>
                      </div>
                    </Popover.Panel>
                  </>
                )}
              </Popover>

              {/* Beds/Baths Popover */}
              <Popover className="relative">
                {({ open }) => (
                  <>
                    <Popover.Button className={`flex items-center gap-2 px-5 py-2.5 rounded-xl border font-bold text-sm transition-all focus:outline-none ${minBeds || minBaths || open
                      ? 'bg-[#09292a] border-teal-600/50 text-teal-400'
                      : 'bg-[#0b0e17] border-[#1d2335] text-slate-200 hover:border-slate-500 hover:bg-[#121622]'
                      }`}>
                      Beds/baths
                      <ChevronDown size={14} className={`transition-transform duration-200 ${open ? 'rotate-180' : ''}`} />
                    </Popover.Button>
                    <Popover.Panel transition className="absolute z-50 mt-2 w-[480px] -left-10 lg:left-0 bg-[#10141e] border border-[#1d2335] rounded-xl shadow-2xl p-6 transition duration-200 ease-out data-[closed]:-translate-y-1 data-[closed]:opacity-0 data-[closed]:scale-95">
                      <div className="flex flex-col gap-6">
                        <div className="flex flex-col gap-3">
                          <label className="text-[15px] font-bold text-slate-200 flex items-baseline gap-2">
                            Beds
                          </label>
                          <div className="flex items-center bg-[#0b0e17] border border-[#1d2335] rounded-xl p-1 overflow-x-auto overflow-y-hidden no-scrollbar">
                            {bedOptions.map((opt, i) => {
                              const isActive = minBeds === opt.value;
                              return (
                                <div key={opt.label} className="flex relative items-center">
                                  <button
                                    type="button"
                                    onClick={() => setMinBeds(opt.value)}
                                    className={`px-4 sm:px-6 py-2 text-sm font-bold rounded-lg transition-all border ${isActive
                                      ? 'bg-[#09292a] border-teal-600/50 text-teal-400'
                                      : 'border-transparent text-slate-300 hover:text-white'
                                      }`}
                                  >
                                    {opt.label}
                                  </button>
                                  {i < bedOptions.length - 1 && (
                                    <div className="w-[1px] h-4 bg-[#1d2335] mx-1" />
                                  )}
                                </div>
                              )
                            })}
                          </div>
                        </div>

                        <div className="flex flex-col gap-3">
                          <label className="text-[15px] font-bold text-slate-200 flex items-baseline gap-2">
                            Baths
                          </label>
                          <div className="flex items-center bg-[#0b0e17] border border-[#1d2335] rounded-xl p-1 overflow-x-auto overflow-y-hidden no-scrollbar">
                            {bathOptions.map((opt, i) => {
                              const isActive = minBaths === opt.value;
                              return (
                                <div key={opt.label} className="flex relative items-center">
                                  <button
                                    type="button"
                                    onClick={() => setMinBaths(opt.value)}
                                    className={`px-4 xl:px-6 py-2 text-sm font-bold rounded-lg transition-all border ${isActive
                                      ? 'bg-[#09292a] border-teal-600/50 text-teal-400'
                                      : 'border-transparent text-slate-300 hover:text-white'
                                      }`}
                                  >
                                    {opt.label}
                                  </button>
                                  {i < bathOptions.length - 1 && (
                                    <div className="w-[1px] h-4 bg-[#1d2335] mx-1" />
                                  )}
                                </div>
                              )
                            })}
                          </div>
                        </div>

                        <div className="flex justify-end gap-3 mt-2 border-t border-[#1d2335] pt-4">
                          <button
                            type="button"
                            onClick={() => { setMinBeds(''); setMinBaths(''); }}
                            className="text-[15px] font-bold text-teal-500 hover:text-teal-400 mr-2"
                          >
                            Reset
                          </button>
                          <Popover.Button className="px-8 py-2.5 bg-[#e93d56] hover:bg-[#d4354c] text-white rounded-lg transition-all font-bold text-sm">
                            Done
                          </Popover.Button>
                        </div>
                      </div>
                    </Popover.Panel>
                  </>
                )}
              </Popover>

              {/* Home Type Popover */}
              <Popover className="relative">
                {({ open }) => (
                  <>
                    <Popover.Button className={`flex items-center gap-2 px-5 py-2.5 rounded-xl border font-bold text-sm transition-all focus:outline-none ${propertyType.length > 0 || open
                      ? 'bg-[#09292a] border-teal-600/50 text-teal-400'
                      : 'bg-[#0b0e17] border-[#1d2335] text-slate-200 hover:border-slate-500 hover:bg-[#121622]'
                      }`}>
                      Home type
                      <ChevronDown size={14} className={`transition-transform duration-200 ${open ? 'rotate-180' : ''}`} />
                    </Popover.Button>
                    <Popover.Panel transition className="absolute z-50 mt-2 w-[340px] -left-10 lg:left-0 bg-[#10141e] border border-[#1d2335] rounded-xl shadow-2xl p-6 transition duration-200 ease-out data-[closed]:-translate-y-1 data-[closed]:opacity-0 data-[closed]:scale-95">
                      <div className="flex flex-col gap-6">
                        <div className="flex gap-4">
                          {typeOptions.map((opt) => {
                            const Icon = opt.icon;
                            const isActive = propertyType.includes(opt.value);
                            return (
                              <button
                                type="button"
                                onClick={() => {
                                  if (isActive) {
                                    setPropertyType(propertyType.filter(pt => pt !== opt.value));
                                  } else {
                                    setPropertyType([...propertyType, opt.value]);
                                  }
                                }}
                                key={opt.label}
                                className={`flex flex-col items-center justify-center w-full aspect-square rounded-xl border transition-all ${isActive
                                  ? 'bg-[#092b2d] border-teal-600/60 text-teal-400'
                                  : 'bg-[#0b0e17] border-[#1d2335] text-slate-300 hover:border-slate-500 hover:text-white'
                                  }`}
                              >
                                <Icon size={32} strokeWidth={isActive ? 2 : 1.5} className="mb-3" />
                                <span className="text-[13px] font-bold">{opt.label}</span>
                              </button>
                            )
                          })}
                        </div>
                        <div className="flex justify-end gap-3 mt-0 border-t border-[#1d2335] pt-4">
                          <button
                            type="button"
                            onClick={() => { setPropertyType([]); }}
                            className="text-[15px] font-bold text-teal-500 hover:text-teal-400 mr-2"
                          >
                            Reset
                          </button>
                          <Popover.Button className="px-8 py-2.5 bg-[#e93d56] hover:bg-[#d4354c] text-white rounded-lg transition-all font-bold text-sm">
                            Done
                          </Popover.Button>
                        </div>
                      </div>
                    </Popover.Panel>
                  </>
                )}
              </Popover>

              <button
                type="submit"
                className="px-6 py-2.5 bg-[#e93d56] hover:bg-[#d4354c] text-white rounded-lg transition-all font-bold text-[15px] shadow-lg shadow-[#e93d56]/10 disabled:opacity-50 ml-auto"
                disabled={loading}
              >
                {loading ? 'Searching...' : 'Apply Filter'}
              </button>
            </div>

          </div>
        </form>

        <main>
          {loading ? (
            <div className="flex justify-center py-20">
              <div className="animate-spin rounded-full h-10 w-10 border-b-2 border-teal-500"></div>
            </div>
          ) : rentals.length === 0 ? (
            <div className="text-center py-20 text-slate-500 bg-[#10141e] border border-[#1d2335] rounded-2xl">
              <Search className="mx-auto h-12 w-12 text-slate-600 mb-4 opacity-50" />
              <p className="text-lg font-medium text-slate-400">No rentals found matching your criteria.</p>
              <p className="text-sm mt-1">Try adjusting your filters to see more results.</p>
            </div>
          ) : (
            <motion.div 
              layout
              className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 xl:grid-cols-4 gap-6"
            >
              <AnimatePresence mode="popLayout">
                {sortedRentals.map((rental) => {
                  const isLiked = likedIds.includes(rental.id);
                  const isDisliked = dislikedIds.includes(rental.id);
                  
                  return (
                    <motion.div
                      layout
                      key={`${rental.source}-${rental.source_id}`}
                      initial={{ opacity: 0, scale: 0.9 }}
                      animate={{ opacity: 1, scale: 1 }}
                      exit={{ opacity: 0, scale: 0.9 }}
                      transition={{ 
                        type: "spring",
                        stiffness: 300,
                        damping: 30,
                        opacity: { duration: 0.2 }
                      }}
                      className={`bg-[#10141e] border rounded-[24px] overflow-hidden transition-all duration-500 group flex flex-col shadow-xl shadow-black/20 relative ${
                        isLiked 
                          ? 'ring-2 ring-teal-500/50 border-teal-500/50' 
                          : isDisliked 
                          ? 'opacity-60 grayscale-[0.5] border-[#1d2335]' 
                          : rental.source.toLowerCase() === 'zillow'
                          ? 'border-[#004fbd]/40 hover:border-[#006aff]/80 hover:shadow-[#006aff]/10'
                          : rental.source.toLowerCase() === 'redfin'
                          ? 'border-[#9c191a]/40 hover:border-[#c82021]/80 hover:shadow-[#c82021]/10'
                          : 'border-[#0f3b39] hover:border-teal-500/60'
                      }`}
                    >
                    {/* Subtle Background Watermark */}
                    <span className="absolute -top-4 -right-2 text-[#151b29] font-black tracking-widest select-none text-8xl pointer-events-none z-0">
                      {rental.source.toUpperCase()}
                    </span>

                    {/* Like/Dislike Quick Actions */}
                    <div className="absolute top-4 right-4 z-20 flex gap-2">
                      <button
                        onClick={() => toggleLike(rental.id)}
                        className={`p-2 rounded-full transition-all ${
                          isLiked 
                            ? 'bg-teal-500 text-white shadow-lg shadow-teal-500/30 ring-4 ring-teal-500/20' 
                            : 'bg-[#0b0e17]/80 text-slate-400 hover:text-teal-400 hover:bg-[#121622]'
                        }`}
                      >
                        <ThumbsUp size={18} fill={isLiked ? "currentColor" : "none"} />
                      </button>
                      <button
                        onClick={() => toggleDislike(rental.id)}
                        className={`p-2 rounded-full transition-all ${
                          isDisliked 
                            ? 'bg-rose-500 text-white shadow-lg shadow-rose-500/30 ring-4 ring-rose-500/20' 
                            : 'bg-[#0b0e17]/80 text-slate-400 hover:text-rose-400 hover:bg-[#121622]'
                        }`}
                      >
                        <ThumbsDown size={18} fill={isDisliked ? "currentColor" : "none"} />
                      </button>
                    </div>

                    {/* Card Details */}
                    <div className="p-6 md:p-7 flex flex-col flex-grow relative z-10">
                      <div className="flex gap-2 mb-5">
                      <div className={`rounded-full px-3.5 py-1.5 text-[10px] font-bold uppercase tracking-widest shadow-lg ${rental.source.toLowerCase() === 'zillow'
                        ? 'bg-[#006aff] border border-[#004fbd] text-white'
                        : rental.source.toLowerCase() === 'redfin'
                          ? 'bg-[#c82021] border border-[#9c191a] text-white'
                          : 'bg-[#042a27] border border-[#094a45] text-[#00e5c1]'
                        }`}>
                        {rental.source}
                      </div>
                      {rental.property_type && (
                        <div className="bg-[#042a27] border border-[#094a45] rounded-full px-3.5 py-1.5 text-[10px] font-bold uppercase tracking-widest text-[#00e5c1] shadow-lg">
                          {rental.property_type}
                        </div>
                      )}
                    </div>

                    <div className="flex justify-between items-center mb-5">
                      <h3 className="text-[28px] font-extrabold tracking-tight text-white flex items-center leading-none">
                        <span className="text-slate-500 font-semibold text-xl mr-2">$</span>
                        {rental.price?.toLocaleString() || 'N/A'}
                      </h3>
                      <div className="flex items-center gap-2.5 text-sm font-bold text-slate-200">
                        <div className="flex items-center gap-2 bg-[#161b26] border border-[#22293b] px-3.5 py-2 rounded-xl">
                          <BedDouble size={16} className="text-[#00e5c1]" />
                          {rental.beds !== null ? (rental.beds % 1 === 0 ? rental.beds : rental.beds) : '-'}
                        </div>
                        <div className="flex items-center gap-2 bg-[#161b26] border border-[#22293b] px-3.5 py-2 rounded-xl">
                          <Bath size={16} className="text-[#00e5c1]" />
                          {rental.baths !== null ? (rental.baths % 1 === 0 ? rental.baths : rental.baths) : '-'}
                        </div>
                      </div>
                    </div>

                    <div className="flex items-start gap-3 mb-4">
                      <MapPin size={16} className="text-slate-400 shrink-0 mt-[2px]" />
                      <p className="text-[14px] text-slate-200 font-medium leading-relaxed">
                        {rental.raw_address} {rental.city && `, ${rental.city}`} {rental.state && `, ${rental.state}`} {rental.zip}
                      </p>
                    </div>

                    <p className="text-[13px] text-slate-500 mb-8 line-clamp-3 leading-relaxed">
                      {rental.description ? rental.description : 'No description provided.'}
                    </p>

                    <div className="mt-auto border-t border-[#1d2335] pt-5 flex justify-between items-center">
                      <span className="text-[12px] font-bold text-slate-400 bg-[#161b26] px-3 py-1.5 rounded-lg border border-[#22293b]">
                        {rental.sqft ? `${rental.sqft.toLocaleString()} sqft` : 'Unknown sqft'}
                      </span>
                      <a
                        href={rental.canonical_url}
                        target="_blank"
                        rel="noreferrer"
                        className="text-[#00e5c1] hover:text-teal-300 font-bold text-[14px] flex items-center gap-1.5 transition-colors group-hover:underline underline-offset-4"
                      >
                        View Details
                        <ExternalLink size={16} strokeWidth={2.5} />
                      </a>
                    </div>
                  </div>
                </motion.div>
              );
            })}
          </AnimatePresence>
            </motion.div>
          )}
        </main>
      </div>
    </div>
  );
}

export default App;
