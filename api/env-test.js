export default {
  fetch(request) {
    const token = process.env.META_ADS_ACCESS_TOKEN;
    return new Response(
      JSON.stringify({
        has_token: !!token,
        token_length: token ? token.length : 0,
        token_start: token ? token.substring(0, 10) : 'none',
      }),
      { headers: { 'Content-Type': 'application/json' } }
    );
  },
};
