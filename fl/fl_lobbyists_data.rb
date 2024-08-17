# Creator:      Alex Bokow
# Migrated by:  Sergii Butrymenko
# Dataset Name: FL Lobbyists Data
# Task #:       31
# Migrated:     June 2021

# ruby mlc.rb --tool="clean::fl::fl_lobbyists_data"

def easy_titleize(line)
  line.split.map(&:capitalize) * ' '
end

def mac_mc(line)
  line = line.sub(/(Mc|Mac) *([^iIoOuUyY]\D*)/) { "#{Regexp.last_match[1]}#{easy_titleize(Regexp.last_match[2])}" }
  line.sub(/(Mc|Mac) +(\D+)/) { "#{Regexp.last_match[1]}#{easy_titleize(Regexp.last_match[2])}" }
end

def normalize(value)
  return value if value.nil? || value.empty?
  if value.upcase == value
    value = value.split(' ').map { |e| e =~ /(-|\.|'|\(|\))/ ? e : e.capitalize }.inject { |a, b| "#{a} #{b}" }
  else
    value = value.split(' ').map { |w| w.upcase == w || w =~ /(-|\.|'|\(|\))/ ? w : w.capitalize }.inject { |a, b| "#{a} #{b}" }
  end
  value.gsub!(/\bof\b/i, 'of')
  value.gsub!(/\band\b/i, 'and')
  value
end

def clean_name(name)
  name.gsub!(/(\s|\A)(([a-z])\.?)(\s|\z)/i){" #{$3.upcase}. "}
  name.gsub!(/\bsr\b\.?/i, 'Sr.')
  name.gsub!(/\bjr\b\.?/i, 'Jr.')
  name.gsub!(/\besq\b\.?/i, 'Esq.')
  name.gsub!(/\bmr\b\.?/i, 'Mr.')
  name.gsub!(/\bmrs\b\.?/i, 'Mrs.')
  name.gsub!(/\bms\b\.?/i, 'Ms.')
  name.gsub!(/\bdr\b\.?/i, 'Dr.')
  name.squeeze(' ').strip
end

def clean_city(city)
  city.gsub!(/(\s|\A)(([a-z])\.?)(\s|\z)/i){" #{$3.upcase}. "}
  city.gsub!(/\bbch\b\.?/i, 'Beach')
  city.gsub!(/\bft\b\.?/i, 'Fort')
  city.gsub!(/\b(is(land)?)\b\.?/i, 'Islands')
  city.gsub!(/\b(st|saint)\b\.?/i, 'St.')
  city.gsub!(/\b(spg|spgs|spring)\b\.?/i, 'Springs')
  city.gsub!(/\bgdns\b\.?/i, 'Gardens')
  city.gsub!(/\b(pt|prt)\b\.?/i, 'Port')
  city.gsub!(/\bhls\b\.?/i, 'Hills')
  city.gsub!(/\b(rch|rnch)\b\.?/i, 'Ranch')
  city.gsub!(/\bgrvs\b\.?/i, 'Groves')
  city.gsub!(/\bpk\b\.?/i, 'Park')
  city.gsub!(/\bvlg\b\.?/i, 'Village')
  city.gsub!(/\bsq\b\.?/i, 'Square')
  city.gsub!(/\blk\b\.?/i, 'Lake')
  city.gsub!(/\bn\b\.? ?/i, 'North ')
  city.gsub!(/\bmt\b\.? ?/i, 'Mount ')
  city.gsub!(/\bsp\b\.?/i, 'Space')
  city.gsub!(/\bct\b\.?/i, 'Center')
  city.gsub!(/\bkng\b\.?/i, 'King')
  city.gsub!(/\bprussa\b\.?/i, 'Prussia')
  city.gsub!(/\bvis\b\.?/i, 'Vista')
  city.gsub!(/\blxhtchee\b\.?/i, 'Loxahatchee')
  city.gsub!(/\brchy\b\.?/i, 'Richey')
  city.gsub!(/\brsa\b\.?/i, 'Rosa')
  city.gsub!(/\bte(r+)\b\.?/i, 'Terrance')
  city.gsub!(/\bsta\b\.?/i, 'Station')
  city.gsub!(/\btwp\b\.?/i, 'Township')
  city.gsub!(/\bcpe\b\.?/i, 'Cape')
  city.gsub!(/\bcv\b\.?/i, 'Cove')
  city.gsub!(/\bplm\b\.?/i, 'Palm')
  city.gsub!(/\bny\b/i, 'New York')
  city.gsub!(/\bsf\b/i, 'San Francisco')
  city.gsub!(/\bchgo\b/i, 'Chicago')
  city.gsub!(/\bkcmo\b/i, 'Kansas City')
  city.gsub!(/\bprov\b/i, 'Providence')
  city.gsub!(/\bsw\b/i, 'Southwest')
  city.gsub!(/\bse\b/i, 'Southeast')
  city.gsub!(/\bnw\b/i, 'Northwest')
  city.gsub!(/\bne\b/i, 'Northeast')
  city.gsub!(/billage/i, 'Village')
  city.gsub!(/mimai/i, 'Miami')
  city.gsub!(/minneapolois/i, 'Minneapolis')
  city.gsub!(/sausalitio/i, 'Sausalito')
  city.gsub!(/franciscosco/i, 'Francisco')
  city.gsub!(/hallandle/i, 'Hallandale')
  city.gsub!(/charlottee/i, 'Charlotte')
  city.gsub!(/boyton/i, 'Boynton')
  city.gsub!(/corla/i, 'Coral')
  city.gsub!(/immokolee/i, 'Immokalee')
  city.gsub!(/peterburg/i, 'Petersburg')
  city.gsub!(/temple terrance/i, 'Temple Terrace')
  city.gsub!(/coral gablea/i, 'Coral Gables')
  city.gsub!(/sunnyu/i, 'Sunny')
  city.gsub!(/\b(washington( d\.?c\.?)?|d\.?c\.?\b)/i, 'Washington')
  city.gsub!(/ma(c+)lenny/i, 'Macclenny')
  city.gsub!(/land(.+)lakes/i, 'Land O\'Lakes')
  city.gsub!(/opa(.+)locka\.?/i, 'Opa-locka')
  city.gsub!(/\bp\.? ?c\b\.?/i, 'Panama City')
  city.gsub!(/tallah(a+)(s+)(e+)/i, 'Tallahassee')
  city.gsub!(/u(.+)saddle(.+)riv(.+)?/i, 'Upper Saddle River')
  city.gsub!(/port(.+)st(.+)lucie/i, 'Port St. Lucie')
  city.gsub!(/\bwe(s+)t\b/i, 'West')
  city.squeeze(' ').strip.chomp(',')
end

def clean_state(state, route)
  state.gsub!(/(D\.C\.|DISTRICT OF COLUMBIA|Washington DC)/i, 'DC')
  state.gsub!(/(\b(fl|fla)\b\.?|florida)/i, 'FL')
  state.gsub!(/\bca\b\.?/i, 'CA')
  state.gsub!(/\b(U.S. State|US)\b/i, 'USA')
  state.gsub!(/\b(Ontario Canada|ON Canada|BC Canada|Quebec Canada)\b/i, 'Canada')
  # state.gsub!(/\bMakabim-Re'ut\b/i, 'Israel')
  state.gsub!(/\bPlurinational State of Bolivia\b/i, 'Bolivia')
  state.gsub!(/\bLisboa Portugal\b/i, 'Portugal')
  state.gsub!(/\bBavaria Germany\b/i, 'Germany')

  state_data = route.query("SELECT short_name, name FROM hle_resources_readonly_sync.usa_administrative_division_states").to_a
  if state.match?(/[a-z]/i)
    if state.size == 2
      state_clean = state
      state_full_a = state_data.select { |e| e['short_name'].downcase == state.downcase }
      state_full = state_full_a.empty? ? state : state_full_a[0]['name']
    else
      state_full = state
      state_clean_a = state_data.select { |e| e['name'].downcase == state.downcase }
      state_clean = state_clean_a.empty? ? state : state_clean_a[0]['short_name']
    end
  else
    state_clean = nil
    state_full = state
  end

  [state_clean, state_full]
end

def clean_org(org)
  org.gsub!(/\b,? inc\b\.?/i, ', Inc.')
  org.gsub!(/\bcorp\b\.?/i, 'Corp.')
  org.gsub!(/\bco\b\.?/i, 'Co.')
  org.gsub!(/\bbros\b\.?/i, 'Bros.')
  org.gsub!(/\b,? p\.? ?a\b\.?/i, ', P.A.')
  org.gsub!(/\b,? p\.?l\b\.?/i, ', PL')
  org.gsub!(/\b,? l\.?l\.?c\b\.?/i, ', LLC')
  org.gsub!(/\b,? l\.?l\.?p\b\.?/i, ', LLP')
  org.gsub!(/\b,? p\.?l\.?l\.?c\b\.?/i, ', PLLC')
  org.gsub!(/\b,? l\.?p\.?a\b\.?/i, ', LPA')
  org.gsub!(/\b,? p\.?c\b\.?/i, ', PC')
  org.gsub!(/\bchtd\b\.?/i, 'Chtd.')
  org.gsub!(/\bltd\b\.?/i, 'Ltd.')
  org.gsub!(/\b(ai|bg|cjt|mmr|aarp|adt|aecom|aglca|agpm|amoaf|at&t|chspsc|crispr|dacco|eecs|fcmc|fmr|fpl|gcom|hms|ibm|icf|iwp|jafco|jea|naf|nfib|nocti|odl|oscr|pdcs|pfs teco|ppsc|psbi|refg|rsm us|rx|fl|sox|tbv|ups|urac|usaa|uzurv|vrbo|wlholdco|wot|wsp)\b\.?/i){$1.upcase}
  org.gsub!(/\bamgen\b/i, 'AMGen')
  org.gsub!(/\bamikids\b/i, 'AMIkids')
  org.gsub!(/\bashbritt\b/i, 'AshBritt')
  org.gsub!(/\bdlrdmv\b/i, 'DLRdmv')
  org.gsub!(/\becoatm\b/i, 'EcoATM')
  org.gsub!(/\befp admin\b/i, 'Efp Admin')
  org.gsub!(/\bfmsbonds\b/i, 'FMSbonds')
  org.gsub!(/\bjdcphosphate\b/i, 'JDCPhosphate')
  org.gsub!(/\bmtoa\b/i, 'MtoA')
  org.gsub!(/\bnextnav\b/i, 'NextNav')
  org.gsub!(/\brhrma\b/i, 'PhRMA')
  org.gsub!(/\bskyetec\b/i, 'SkyeTec')
  org.gsub!(/\btmaxsoft\b/i, 'TmaxSoft')
  org.gsub!(/\bunidosus\b/i, 'UnidosUS')
  org.gsub!(/\bvmware\b/i, 'VMware')
  org.gsub!(/\bweedmaps\b/i, 'WeedMaps')
  org.gsub!(/\bwellsky\b/i, 'WellSky')
  org.gsub!(/\bwework\b/i, 'WeWork')
  org.gsub!(/\bsr\.? ii\b/i, 'SR II')
  org.gsub!(/\bwoz u\b\.?/i, 'Woz U')
  org.gsub!(/\bd\/?b\/?a\b/i, 'DBA')
  if org =~ /(, the|\(the\))/i
    org.gsub!(/(, the|\(the\))/i, '')
    org = 'The ' + org
  end
  org.squeeze(' ').strip
end

def update_states_table(route)
  query = <<~SQL
    SELECT t.state FROM
      (
        SELECT DISTINCT state
        FROM fl_lobbyist_info
        WHERE cleaned IS NULL OR cleaned<>1
        UNION
        SELECT DISTINCT state
        FROM fl_lobbyist_firm_info
        WHERE cleaned IS NULL OR cleaned<>1
        UNION
        SELECT DISTINCT state
        FROM fl_lobbyist_principals
        WHERE cleaned IS NULL OR cleaned<>1
      ) t
      LEFT JOIN fl_lobbyist__states_clean c ON c.state=t.state
    WHERE c.state IS NULL;
  SQL
  states_to_clean = route.query(query).to_a
  states_to_clean.each do |item|
    state = item['state']
    state_clean, state_full = clean_state(state.dup, route)
    puts "CLEAN: #{state} => #{state_clean}".green
    puts "FULL_: #{state} => #{state_full}".cyan
    query = <<~SQL
      INSERT IGNORE INTO fl_lobbyist__states_clean (state, state_clean, state_full)
      VALUES ('#{escape(state)}', '#{escape(state_clean)}', '#{escape(state_full)}');
    SQL
    puts query.red
    route.query(query)
  end
  st_count = states_to_clean.count
  message_to_slack(st_count > 0 ? "#{st_count} were cleaned and added to *db01.usa_raw.fl_lobbyist__states_clean*. Please check them and update skip_it column were needed." : 'No new states were found')
end

def message_to_slack(message)
  Slack::Web::Client.new.chat_postMessage(
      channel: 'UKLB1JGDN',
      text: "*[CLEANING #31] FL Lobbyists Data* \n>#{message}",
      as_user: true
  )
end

def execute(options = {})
  route = C::Mysql.on(DB01, 'usa_raw')

  update_states_table(route)
  # states_to_clean = []

  cities = route.query(<<~SQL
    SELECT short_name city
    FROM hle_resources_readonly_sync.usa_administrative_division_counties_places_matching
    GROUP BY city;
  SQL
  ).to_a

  zips = route.query(<<~SQL
    SELECT primary_city city, zip
    FROM hle_resources_readonly_sync.zipcode_data;
  SQL
  ).to_a

  puts 'CLEAN LOBBYISTS'.cyan

  lobbyists = route.query(<<~SQL
    SELECT id,
           CONCAT(first_name, ' ', last_name) lobbyist,
           city,
           LEFT(zip, 5) zip
           # ,state
    FROM fl_lobbyist_info
    WHERE cleaned IS NULL;
  SQL
  ).to_a

  lobbyists.each_with_index do |l, i|
    puts "#{i + 1}/#{lobbyists.count} :"

    lobbyist = l['lobbyist']
    lobbyist_clean = clean_name(mac_mc(normalize(lobbyist)))
    puts "#{lobbyist} => #{lobbyist_clean}"

    city = l['city']
    city_clean = clean_city(mac_mc(normalize(city)))
    puts "#{city} => #{city_clean}"

    p city_arr = cities.select { |e| e['city'] == city_clean }

    city_matched = 0
    zip_matched = 0
    if city_arr.empty?
      p zip_arr = zips.select { |e| e['zip'] == l['zip'].to_i }

      if !zip_arr.empty?
        zip_matched = 1
        p zip_city = zip_arr[0]['city']
      end
    else
      city_matched = 1
    end

    # state = l['state']
    # state_clean, state_full = clean_state(state, route)
    # puts "#{state} => #{state_clean}"
    # puts "#{state} => #{state_full}"
    # states_to_clean |= {'state' => state, 'state_clean' => 'state_clean', 'state_full' => state_full}

    route.query(<<~SQL
      UPDATE fl_lobbyist_info
      SET lobbyist_clean_name = '#{escape(lobbyist_clean)}',
          city_clean = '#{escape(city_clean)}',
          city_matched = #{city_matched},
          zip_matched = #{zip_matched},
          zip_city = '#{escape(zip_city) if !zip_city.nil?}',
          cleaned = 1
      WHERE id = #{l['id']};
    SQL
    )
          # state_clean = '#{escape(state_clean)}',
          # state_full = '#{escape(state_full)}',
  end

  puts '################'

  puts 'CLEAN FIRMS'.cyan

  firms = route.query(<<~SQL
    SELECT id,
           lobbyist_firm firm,
           city,
           LEFT(zip, 5) zip
           # ,state
    FROM fl_lobbyist_firm_info
    WHERE cleaned IS NULL;
  SQL
  ).to_a

  firms.each_with_index do |f, i|
    puts "#{i + 1}/#{firms.count} :"

    firm = f['firm']
    firm_clean = clean_org(clean_city(clean_name(mac_mc(normalize(firm)))))
    puts "#{firm} => #{firm_clean}"

    city = f['city']
    city_clean = clean_city(mac_mc(normalize(city)))
    puts "#{city} => #{city_clean}"

    city_arr = cities.select { |e| e['city'] == city_clean }

    city_matched = 0
    zip_matched = 0
    if city_arr.empty?
      zip_arr = zips.select { |e| e['zip'] == f['zip'].to_i }

      if !zip_arr.empty?
        zip_matched = 1
        zip_city = zip_arr[0]['city']
      end
    else
      city_matched = 1
    end

    # state = f['state']
    # state_clean, state_full = clean_state(state, route)
    # puts "#{state} => #{state_clean}"
    # puts "#{state} => #{state_full}"
    # states_to_clean |= {'state' => state, 'state_clean' => 'state_clean', 'state_full' => state_full}

    route.query(<<~SQL
      UPDATE fl_lobbyist_firm_info
      SET lobbyist_firm_clean = '#{escape(firm_clean)}',
      city_clean = '#{escape(city_clean)}',
      city_matched = #{city_matched},
      zip_matched = #{zip_matched},
      zip_city = '#{escape(zip_city) if !zip_city.nil?}',
      cleaned = 1 WHERE id = #{f['id']};
    SQL
    )
      # state_clean = '#{escape(state_clean)}',
      # state_full = '#{escape(state_full)}',
  end

  puts '################'

  puts 'CLEAN PRINCIPALS'.cyan

  principals = route.query(<<~SQL
    SELECT id,
           principal_name principal,
           city,
           LEFT(zip, 5) zip
           # ,state
    FROM fl_lobbyist_principals
    WHERE cleaned IS NULL;
  SQL
  ).to_a

  principals.each_with_index do |p, i|
    puts "#{i + 1}/#{principals.count} :"

    principal = p['principal']
    principal_clean = clean_org(clean_city(clean_name(mac_mc(normalize(principal)))))
    puts "#{principal} => #{principal_clean}"

    city = p['city']
    city_clean = clean_city(mac_mc(normalize(city)))
    puts "#{city} => #{city_clean}"

    city_arr = cities.select { |e| e['city'] == city_clean }

    city_matched = 0
    zip_matched = 0
    if city_arr.empty?
      zip_arr = zips.select { |e| e['zip'] == p['zip'].to_i }

      if !zip_arr.empty?
        zip_matched = 1
        zip_city = zip_arr[0]['city']
      end
    else
      city_matched = 1
    end
    zip5 = p['zip'].match?(/\d{5}/) ? p['zip'] : 'NULL'


    # state = p['state']
    # state_clean, state_full = clean_state(state, route)
    # puts "#{state} => #{state_clean}"
    # puts "#{state} => #{state_full}"
    # states_to_clean |= {'state' => state, 'state_clean' => 'state_clean', 'state_full' => state_full}

    route.query(<<~SQL
      UPDATE fl_lobbyist_principals
      SET principal_name_clean = '#{escape(principal_clean)}',
          city_clean = '#{escape(city_clean)}',
          city_matched = #{city_matched},
          zip_matched = #{zip_matched},
          zip5 = #{zip5},
          zip_city = '#{escape(zip_city) if !zip_city.nil?}',
          cleaned = 1
      WHERE id = #{p['id']};
    SQL
    )
          # state_clean = '#{escape(state_clean)}',
          # state_full = '#{escape(state_full)}',
  end

  # update_states_table(states_to_clean, route)

  route.close
end
