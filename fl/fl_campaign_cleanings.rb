# [bundle exec] ruby mlc.rb --tool='clean::fl::fl_campaign_cleanings' --cleaning_type=1
# cleaning types are between 1 and 2 according to the required files
require_relative 'fl_campaign/expense_purposes'
require_relative 'fl_campaign/payees'
require_relative 'fl_campaign/testing_cleanings'

WEBSITES_REGEXP = /\.(com|net|org)/i

def execute(options = {})
  if options['cleaning_type']
    case options['cleaning_type']
    when '1'; expense_purposes_cleaning
    when '2'; payees_cleaning
    when '3'; testing_cleaning
    else puts "Wrong --cleaning_type, Execute cleaning with --cleaning_type = one of (1..2)"
    end
  else
    puts "No --cleaning_type, Execute cleaning with --cleaning_type = one of (1..2)"
  end
end

def hyphen_space_exceptions
  /\b((\d+ - \d+)|(\w - (\w+|\d+))|(\w+ - \w))\b/i
end

def serv_replacement_hsh
  {/^(ST\.?|SAINT)$/i => 'St.',
   /^MT\.?$/i => 'Mount',
   /^HIALEAH$/i => 'Hialeah',
   /^EX(PS)?$/i => 'expenses',
   /^DIST\.?$/i => 'district',
   /^MGMT\.?$/i => 'management',
   /^COMM\.?$/i => 'committee',
   /^(FL|FLORIDA)$/i => 'Florida',
   /^REIM(B)?$/i => 'Reimbursment',
   /^(PETE|PETERSBURG)$/i => 'Petersburg',
   /^CO(RP)?$/i => 'Corp.',
   /^INC\.$/ => 'Inc.',
   /^LUICE$/ => 'Luice',
   /^(CONTRIBUTIO|CONTRIBUTON)$/ => 'contribution',
   /^MIAMI$/ => 'Miami',
   /^ORLANDO$/ => 'Orlando',
   /^PATRONIS$/ => 'Patronis',
   /^MIRAMAR$/ => 'Miramar',
   /^SRVS$/ => 'services',
   /^PINELLAS$/ => 'Pinellas',
   /^PASCO$/ => 'Pasco',
   /^GOV$/ => 'Gov.',
   /^GRP$/ => 'group'
  }
end

def upcase_abbrs
  str = '\b('\
        'AD|ADS|AG|AIF|'\
        'BOYO|BYRD|'\
        'CA|CC|CCD|CDD|CFO|CHG|CPA|CRM|'\
        'DEM|DOE|DLD|'\
        'ECO|'\
        'FDOS|FED|FMA|FOAPAC|'\
        'HD|'\
        'IRS|IT|'\
        'MBR|MDC|MI|'\
        'PAC|PAF|PC|PG|PO|'\
        'REP|RPAC|'\
        'SD|'\
        'TPA|'\
        '[A-Z]{1,10}-?[0-9]{1,10}'\
        '(I{2,3}|I{0,1}[VX]I{0,3})'\
        ')(\.|\b)'
  /#{str}/i
end

def clamped_comma(line)
  line.gsub(/(\S)( ?, ?)(\S)/) { "#{$1}, #{$3}" }
end

def corrupted_divide_sign(line, space = ' ')
  line.gsub(/(\S)( ?\/ ?)(\S)/) { "#{$1}#{space}/#{space}#{$3}" }
end

def corrupted_dot(line)
  line.gsub(/(\S)( ?\. ?)(\S)/) { "#{$1}. #{$3}" }
end

def corrupted_hyphen(line, space = ' ')
  fg_dash = "#{8210.chr(Encoding::UTF_8)}"
  en_dash = "#{8211.chr(Encoding::UTF_8)}"
  em_dash = "#{8212.chr(Encoding::UTF_8)}"
  hor_bar = "#{8213.chr(Encoding::UTF_8)}"
  str = "(#{fg_dash}|#{en_dash}|#{em_dash}|#{hor_bar}|-)"

  line.gsub(/(\S)( ?)#{str}( ?)(\S)/) { "#{$1}#{space}#{$3}#{space}#{$5}" }
end

def corrupted_quote(line)
  line.gsub(/(\S)( ?\' ?)(\S)/) { "#{$1}'#{$3}" }
end

def mac_mc(line)
  return '' if line.to_s == ''
  line.split(' ').map! do |word|
    word.sub(/^(MAC)([^aeiou]{1})(\w*)$/i) { "#{$1.capitalize}#{$2.upcase}#{$3}" }
        .sub(/^(MC)(\w{1})(\w*)$/i)        { "#{$1.capitalize}#{$2.upcase}#{$3}" }
        .sub(/^MacK$/, 'Mack')
  end.join(' ')
end

def quote_rule(line)
  line.gsub(/(\w+\')([^\W|$]*)/i) { "#{$1.capitalize}#{"#{$2.upcase}" == 'S' ? 's' : "#{$2.capitalize}"}" }
end

def pubs_matching_empty(city, state, pubs_route)
  checking_query = <<~SQL
    SELECT DISTINCT
      mat.short_name AS city
    FROM usa_administrative_division_counties_places_matching AS mat
    JOIN usa_administrative_division_states AS st
      ON st.name = mat.state_name
    WHERE st.short_name = #{state.dump}
      AND mat.short_name = #{city.dump};
  SQL
  pubs_route.client.query(checking_query).to_a.empty?
end
